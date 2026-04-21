import os
import logging
import signal
import time
import heapq

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
        # Create input exchange
        INPUT_EXCHANGE_PREFETCH_COUNT = 5
        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{ID}"], prefetch_count=INPUT_EXCHANGE_PREFETCH_COUNT
        )

        # Create output queue
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        # Set initial storad

        # Set initial stored data
        self.agg_data_by_client = {}

        # Assign signal handlers
        signal.signal(signalnum=signal.SIGTERM, handler=self._sigterm_handler)

    def __add_new_client_info(self, sender_id):
        self.agg_data_by_client[sender_id] = {}
        self.agg_data_by_client[sender_id]["eofs_received"] = 0
        self.agg_data_by_client[sender_id]["fruits"] = {}

    def __get_client_fruits(self, sender_id):
        return self.agg_data_by_client[sender_id]["fruits"]

    # Process message gotten from sum
    def _process_data(self, sender_id, fruit, amount):
        logging.info(f"Processing data message: {sender_id}, {fruit}, {amount}")

        # If client never sent data before, store new config
        if sender_id not in self.agg_data_by_client:
            self.__add_new_client_info(sender_id)

        # Add new fruits
        fruits_stored = self.agg_data_by_client[sender_id]["fruits"]
        fruits_stored[fruit] = fruits_stored.get(fruit, fruit_item.FruitItem(fruit, 0)) + fruit_item.FruitItem(fruit, amount)


    def _send_fruits_top(self, sender_id):
        logging.info("Top finished. Sending to joiner...")
        fruits_stored = self.__get_client_fruits(sender_id)

        fruit_chunk = list(fruits_stored.values())

        heapq.heapify_max(fruit_chunk)

        fruit_top = [(item.fruit, item.amount) for item in fruit_chunk]

        fruit_top.append(("sender_id", sender_id))
        self.output_queue.send(message_protocol.internal.serialize(fruit_top))


    def _process_eof(self, sender_id):
        logging.info(f"Processing EOF")

        if sender_id not in self.agg_data_by_client:
            self.__add_new_client_info(sender_id)

        self.agg_data_by_client[sender_id]["eofs_received"] += 1

        if self.agg_data_by_client[sender_id]["eofs_received"] == SUM_AMOUNT:
            self._send_fruits_top(sender_id)

    # Sigterm handler
    def _sigterm_handler(self, signum, frame):
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
                break

            except:
                retry_time = self.__get_shutdown_retry_backoff(current_retries)
                time.sleep(retry_time)
                current_retries += 1

    def process_messsage(self, message, ack, nack):
        logging.info("Process message")
        if message is not None:
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
        self.input_exchange.start_consuming(self.process_messsage)


def main():
    logging.basicConfig(level=logging.INFO)
    aggregation_filter = AggregationFilter()
    aggregation_filter.start()
    return 0


if __name__ == "__main__":
    main()
