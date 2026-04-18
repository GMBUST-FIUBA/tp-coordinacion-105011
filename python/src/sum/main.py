import os
import signal
import logging
import threading
import time
import random
import uuid
import collections

from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
SUM_CONTROL_EXCHANGE = "SUM_CONTROL_EXCHANGE"
SUM_CONTROL_EXCHANGE_KEY = "SUM_CONTROL"
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]

# Control messages
class SumControl:
    EOF_RECV = 1

# Sum class
class SumFilter:
    def __init__(self):
        # Create fruit records queue
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )

        # Create sums control exchanges
        self.control_exchange_sender = middleware.MessageMiddlewareExchangeRabbitMQ(MOM_HOST, SUM_CONTROL_EXCHANGE, [SUM_CONTROL_EXCHANGE_KEY])
        self.control_exchange_receiver = middleware.MessageMiddlewareExchangeRabbitMQ(MOM_HOST, SUM_CONTROL_EXCHANGE, [SUM_CONTROL_EXCHANGE_KEY])

        # Lock to control the last control message
        self.last_ctrl_message_lock = threading.Lock()
        self.last_ctrl_message = None

        # Control to say if data was stored and sent
        self.data_was_sent = False

        # Store order of EOF received
        self.agg_sending_order = collections.deque()

        # Create output exchanges
        self.data_output_exchanges = []
        for i in range(AGGREGATION_AMOUNT):
            data_output_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{i}"]
            )
            self.data_output_exchanges.append(data_output_exchange)
        self.fruits_by_id = {}

        # Assign signal handlers
        signal.signal(signalnum=signal.SIGTERM, handler=self._sigterm_handler)

        # Start and store control messages thread
        self.control_msg_input_thread = threading.Thread(target=self._read_control_message)
        self.control_msg_input_thread.start()

    def _read_control_message(self):
        logging.info(f"Input ctrl: start")
        INACTIVITY_TIMEOUT = 1.0
        self.keep_reading_ctrl = True

        while self.keep_reading_ctrl:
            logging.info(f"Input ctrl: start consuming")
            self.control_exchange_receiver.start_consuming(on_message_callback=self.__control_message_callback, inactivity_timeout=INACTIVITY_TIMEOUT)

    def __control_message_callback(self, message, ack, nack):
        try:
            if message is not None:
                # Get message type and arguments
                split_msg = message_protocol.internal.deserialize(message)

                with self.last_ctrl_message_lock:
                    # Check control command
                    msg_type = int(split_msg[0])

                    # If type is EOF store ID for ordering
                    if msg_type == SumControl.EOF_RECV:
                        self.agg_sending_order.append(split_msg[1])
                        self.last_ctrl_message = SumControl.EOF_RECV
                ack()
            elif not self.keep_reading_ctrl:
                self.control_exchange_receiver.stop_consuming()
                logging.info(f"Input ctrl: shutdown")
        except:
            with self.last_ctrl_message_lock:
                self.last_ctrl_message = None
            nack()

        logging.info(f"Input ctrl: callback executed")


    def _process_data(self, sender_id, fruit, amount):
        logging.info(f"Process data: {sender_id}, {fruit}, {amount}")
        
        # Store new sender ID if not present
        if sender_id not in self.fruits_by_id:
            self.fruits_by_id[sender_id] = {}

        # Store new ammount
        amount_by_fruit = self.fruits_by_id[sender_id]
        amount_by_fruit[fruit] = amount_by_fruit.get(
            fruit, fruit_item.FruitItem(fruit, 0)
        ) + fruit_item.FruitItem(fruit, int(amount))

    # Send data to aggregation stage
    def _send_data_to_aggregation(self):
        logging.info(f"Distribuiting data messages")

        # For every sender in order
        for sender_id in self.agg_sending_order:
            # Get aggregator to send records
            sender_id_uuid = uuid.UUID(sender_id)
            dest_agg = sender_id_uuid.int % AGGREGATION_AMOUNT

            # Send data
            for final_fruit_item in self.fruits_by_id[sender_id].values():
                self.data_output_exchanges[dest_agg].send(
                    message_protocol.internal.serialize(
                        [sender_id, final_fruit_item.fruit, final_fruit_item.amount]
                    )
                )

            # Send EOF
            logging.info(f"Sending EOF message to aggregator {dest_agg}")
            self.data_output_exchanges[dest_agg].send(message_protocol.internal.serialize([sender_id]))

    # Process EOF
    # Sends EOF to the rest of the sums
    def _process_eof(self, sender_id):
        logging.info(f"Process msg: EOF received")

        # Propagate EOF to other sums
        self.control_exchange_sender.send(message_protocol.internal.serialize([SumControl.EOF_RECV, sender_id]))

    # Sigterm handler
    def _sigterm_handler(self, signum, frame):
        self.shutdown()

    def __get_shutdown_retry_backoff(self, current_retries):
        RETRY_SHUT_DOWN_TIME_SEC = 0.1
        return RETRY_SHUT_DOWN_TIME_SEC

    def process_data_messsage(self, message, ack, nack):

        # If timeout occurred
        if message is None:
            with self.last_ctrl_message_lock:
                # If a timeout occurred, then clients stopped sending data.
                # Otherwise, shutdown.
                if not self.data_was_sent:
                    if self.last_ctrl_message is SumControl.EOF_RECV:
                        logging.info(f"Process msg: send data to agg")
                        self._send_data_to_aggregation()
                        self.data_was_sent = True
                    else:
                        logging.info(f"Process msg: shutdown because of lost connection")
                        self.shutdown()
        else:
            # If a message is received, process it.
            fields = message_protocol.internal.deserialize(message)
            if len(fields) == 3:
                self._process_data(*fields)
                ack()
            elif len(fields) == 1:
                self._process_eof(*fields)
                ack()
            else:
                nack()

    def shutdown(self):
        MAX_SHUTDOWN_RETRIES = 3
        current_retries = 0

        # Try up to MAX_SHUTDOWN_RETRIES
        while current_retries < MAX_SHUTDOWN_RETRIES:
            try:
                # Close input queue
                self.input_queue.close()

                logging.info(f"Input queue shutdown")

                # Close control messages input
                self.keep_reading_ctrl = False
                self.control_msg_input_thread.join()

                logging.info(f"Control msg input thread shutdown")

                # Close data outputs
                for data_output in self.data_output_exchanges:
                    data_output.close()

                logging.info(f"Successful shutdown")

            except:
                retry_time = self.__get_shutdown_retry_backoff(current_retries)
                time.sleep(retry_time)
                current_retries += 1

    CLIENTS_STOPPED_SENDING_DATA_SECS = 5.0

    def start(self):
        self.input_queue.start_consuming(self.process_data_messsage, inactivity_timeout=SumFilter.CLIENTS_STOPPED_SENDING_DATA_SECS)

def main():
    logging.basicConfig(level=logging.INFO)
    sum_filter = SumFilter()
    sum_filter.start()
    return 0


if __name__ == "__main__":
    main()
