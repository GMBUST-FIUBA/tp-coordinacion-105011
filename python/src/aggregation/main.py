import os
import logging
import bisect
import signal
import time

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

        # Assign signal handlers
        signal.signal(signalnum=signal.SIGTERM, handler=self._sigterm_handler)

    # Reset all storaged data from client
    def _reset_storage(self):
        self.fruit_stored = {}
        self.sender_id = None
        self.eof_received = False

    # Correctly add first fruit record
    def __add_first_fruit_record(self, sender_id, fruit, amount):
        self.sender_id = sender_id
        self.fruit_stored[fruit] = self.fruit_stored.get(fruit, fruit_item.FruitItem(fruit, 0)) + fruit_item.FruitItem(fruit, amount)

    # Correctly store new first fruit record
    def __add_fruit_record(self, fruit, amount):
        self.fruit_stored[fruit] = self.fruit_stored.get(fruit, fruit_item.FruitItem(fruit, 0)) + fruit_item.FruitItem(fruit, amount)
        self.eof_received = False

    # Process message gotten from sum
    def _process_data(self, sender_id, fruit, amount):
        logging.info(f"Processing data message: {sender_id}, {fruit}, {amount}")

        if self.sender_id is None:
            # Fruit top is empty because of recent storage reset
            logging.info(f"Nuevo cliente: {sender_id}")
            self.__add_first_fruit_record(sender_id, fruit, amount)

        elif self.sender_id == sender_id:
            # Fruit record has to be added correctly
            self.__add_fruit_record(fruit, amount)

        else:
            # If an EOF was detected then send top to joiner and add record to empty storage
            # Otherwise, ignore it
            if self.eof_received:
                # Send top to joiner
                self._send_fruits_top()

                # Add record from new client
                self.__add_first_fruit_record(sender_id, fruit, amount)


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

    def _process_eof(self, sender_id):
        logging.info(f"Processing EOF")
        if self.sender_id == sender_id:
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
            if len(fields) == 3:
                self._process_data(*fields)
                ack()
            elif len(fields) == 1:
                self._process_eof(*fields)
                ack()
            else:
                nack()

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
