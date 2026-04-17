import os
import logging
import bisect
import signal
import time
import collections

from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])


class AggregationFilter:

    MAX_NEW_CLIENTS_MESSAGES = 20

    def __init__(self):
        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{ID}"]
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )

        # Set initial stored data
        self.fruit_stored = {}
        self.fruit_top = []
        self.sender_id = None
        self.eof_received = False
        self.total_eof_received = 0

        # Set buffer for incoming messages from other clients
        self.next_client = None
        self.next_clients_messages_pos_in_buffer = {}
        self.next_clients_messages_buffer = collections.deque()
        self.total_stored_msgs = 0

        # Assign signal handlers
        signal.signal(signalnum=signal.SIGTERM, handler=self._sigterm_handler)

    # Reset all storaged data from client
    def _reset_storage(self):
        self.fruit_stored = {}
        self.sender_id = None
        self.eof_received = False
        self.total_eof_received = 0

    # Correctly add first fruit record
    def __add_first_fruit_record(self, sender_id, fruit, amount):
        self.sender_id = sender_id
        self.fruit_stored[fruit] = self.fruit_stored.get(fruit, fruit_item.FruitItem(fruit, 0)) + fruit_item.FruitItem(fruit, amount)

    # Correctly store new first fruit record
    def __add_fruit_record(self, fruit, amount):
        self.fruit_stored[fruit] = self.fruit_stored.get(fruit, fruit_item.FruitItem(fruit, 0)) + fruit_item.FruitItem(fruit, amount)
        self.eof_received = False

    # Store message from another client in buffer
    def __store_other_client_message(self, msg_parts, change_of_client=False):

        if change_of_client is False and self.total_stored_msgs >= self.MAX_NEW_CLIENTS_MESSAGES:
            logging.info(f"ERROR: Too many messages stored")
            self.shutdown()
            raise Exception("Too many messages stored")

        sender_id = msg_parts[0]

        # If sender is sending the first message
        if sender_id not in self.next_clients_messages_pos_in_buffer:
            # Add next client if needed
            if self.next_client is None:
                self.next_client = sender_id

            # Store position of messages to sender ID
            new_pos = len(self.next_clients_messages_pos_in_buffer.keys())
            self.next_clients_messages_pos_in_buffer[sender_id] = new_pos

            # Add message
            self.next_clients_messages_buffer.append([msg_parts[1:]])
        else:
            # The sender already sent a message, so get position in buffer and store message in order
            pos_in_buffer = self.next_clients_messages_pos_in_buffer[sender_id]
            self.next_clients_messages_buffer[pos_in_buffer].append(msg_parts[1:])

        # Store total current stored messages
        self.total_stored_msgs += 1


    # Process message gotten from sum
    def _process_data(self, sender_id, fruit, amount):
        logging.info(f"Processing data message: {sender_id}, {fruit}, {amount}")

        if self.sender_id is None:
            # Fruit top is empty because of recent storage reset
            logging.info(f"Nuevo cliente: {sender_id}")
            self.__add_first_fruit_record(sender_id, fruit, amount)

        else:
            # Fruit record has to be added correctly
            self.__add_fruit_record(fruit, amount)


    def _send_fruits_top(self):
        logging.info("Top finished. Sending to joiner...")
        fruit_chunk = list(self.fruit_stored.values())
        fruit_chunk.sort(reverse=True)
        fruit_top = list(
            map(
                lambda fruit_item: (fruit_item.fruit, fruit_item.amount),
                fruit_chunk[0:TOP_SIZE],
            )
        )
        fruit_top.append(("sender_id", self.sender_id))
        self.output_queue.send(message_protocol.internal.serialize(fruit_top))

        # Restore storage to accept new client data
        self._reset_storage()

    def _send_top_and_accept_next_client_data(self, msg_parts):
        # Send top to joiner
        self._send_fruits_top()

        # Add record from new client
        self.__store_other_client_message(msg_parts, change_of_client=True)

        # Process next client

        ## Remove next client ID from waiting list
        self.next_clients_messages_pos_in_buffer.pop(self.next_client)

        ## Iterate over stored messages
        next_client_messages = self.next_clients_messages_buffer.popleft()
        self.total_stored_msgs -= len(next_client_messages)

        for msg_args in next_client_messages:
            fields = [self.next_client] + msg_args
            if len(fields) == 3:
                self._process_data(*fields)
            elif len(fields) == 1:
                self._process_eof(*fields)

        ## Change all positions stored for buffer
        self.next_client = None
        for id in self.next_clients_messages_pos_in_buffer:
            self.next_clients_messages_pos_in_buffer[id] -= 1

            # Set new next client to handle
            if self.next_clients_messages_pos_in_buffer[id] == 0:
                self.next_client = id


    def _process_eof(self, sender_id):
        logging.info(f"Processing EOF")
        self.total_eof_received += 1
        if self.total_eof_received == SUM_AMOUNT:
            self.eof_received = True

    # Sigterm handler
    def _sigterm_handler(self):
        self.shutdown()

    # Retry backoff when it shutdowns
    def __get_shutdown_retry_backoff(self, current_retries):
        RETRY_SHUT_DOWN_TIME_SEC = 0.2
        return RETRY_SHUT_DOWN_TIME_SEC

    # Shutdown method
    def shutdown(self):
        MAX_SHUTDOWN_RETRIES = 3
        current_retries = 0

        # Try up to MAX_SHUTDOWN_RETRIES
        while current_retries < MAX_SHUTDOWN_RETRIES:
            try:
                # Close input queue
                self.input_exchange.close()
                logging.info(f"Input exchange shutdown")

                # Close data output queue
                self.output_queue.close()

                logging.info(f"Successful shutdown")

            except:
                retry_time = self.__get_shutdown_retry_backoff(current_retries)
                time.sleep(retry_time)
                current_retries += 1

    def process_messsage(self, message, ack, nack):
        logging.info("Process message")
        if message is None:
            if self.eof_received:
                self._send_fruits_top()
        else:
            fields = message_protocol.internal.deserialize(message)
            sender_id = fields[0]
            if self.sender_id is None or sender_id == self.sender_id:
                if len(fields) == 3:
                    self._process_data(*fields)
                    ack()
                elif len(fields) == 1:
                    self._process_eof(*fields)
                    ack()
                else:
                    nack()
            else:
                # Si ya se recibieron todos los EOF, enviar los datos y procesar los de este cliente
                if self.eof_received is True:
                    self._send_top_and_accept_next_client_data(fields)
                else:
                    # Si todavía se espera el EOF de algún sumador, almacenar el mensaje
                    self.__store_other_client_message(fields)

    def start(self):
        INACTIVITY_TIMEOUT_SECS = 1
        self.input_exchange.start_consuming(self.process_messsage, inactivity_timeout=INACTIVITY_TIMEOUT_SECS)


def main():
    logging.basicConfig(level=logging.INFO)
    aggregation_filter = AggregationFilter()
    aggregation_filter.start()
    return 0


if __name__ == "__main__":
    main()
