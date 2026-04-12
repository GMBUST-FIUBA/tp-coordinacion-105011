import os
import signal
import logging
import threading
import time
import random

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
        self.control_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(MOM_HOST, SUM_CONTROL_EXCHANGE, [SUM_CONTROL_EXCHANGE_KEY])
        self.last_ctrl_message = None

        # Create output exchanges
        self.data_output_exchanges = []
        for i in range(AGGREGATION_AMOUNT):
            data_output_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{i}"]
            )
            self.data_output_exchanges.append(data_output_exchange)
        self.amount_by_fruit = {}

        # Assign signal handlers
        signal.signal(signalnum=signal.SIGTERM, handler=self._sigterm_handler)

    def _process_data(self, fruit, amount):
        logging.info(f"Process data")
        self.amount_by_fruit[fruit] = self.amount_by_fruit.get(
            fruit, fruit_item.FruitItem(fruit, 0)
        ) + fruit_item.FruitItem(fruit, int(amount))

    def _process_eof(self):
        logging.info(f"Distribuiting data messages")
        # Get maximum length of chunk
        chunk_max_length = len(self.amount_by_fruit.values()) // AGGREGATION_AMOUNT

        # Set for initial sent data
        available_exchanges = set(range(0, AGGREGATION_AMOUNT))
        current_sent_data_total = 0
        exchange_idx = random.randrange(available_exchanges)

        # For every piece of data
        for final_fruit_item in self.amount_by_fruit.values():
            # Reset output exchange settings for next exchange
            if current_sent_data_total == chunk_max_length:
                available_exchanges.remove(exchange_idx)
                exchange_idx = exchange_idx = random.randrange(available_exchanges)
                current_sent_data_total = 0

            # Send data
            self.data_output_exchanges[exchange_idx].send(
                message_protocol.internal.serialize(
                    [final_fruit_item.fruit, final_fruit_item.amount]
                )
            )
            current_sent_data_total += 1

        # Signal EOF to other sums
        self.control_exchange.send(SumControl.EOF_RECV)
        self.last_ctrl_message = SumControl.EOF_RECV

        # Send EOFs
        logging.info(f"Broadcasting EOF message")
        for data_output_exchange in self.data_output_exchanges:
            data_output_exchange.send(message_protocol.internal.serialize([]))

    def _sigterm_handler(self, signum, frame):
        MAX_SHUTDOWN_RETRIES = 3
        current_retries = 0

        # Try up to MAX_SHUTDOWN_RETRIES
        while current_retries < MAX_SHUTDOWN_RETRIES:
            try:
                # Close input queue
                self.input_queue.close()

                # Close data outputs
                for data_output in self.data_output_exchanges:
                    data_output.close()

                # Close control exchange
                self.control_exchange.close()
            except:
                retry_time = self.__get_shutdown_retry_backoff(current_retries)
                time.sleep(retry_time)
                current_retries += 1
        pass

    def __get_shutdown_retry_backoff(self, current_retries):
        RETRY_SHUT_DOWN_TIME_SEC = 0.1
        return RETRY_SHUT_DOWN_TIME_SEC

    def process_data_messsage(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)
        if len(fields) == 2:
            self._process_data(*fields)
        else:
            self._process_eof(*fields)
        ack()

    def start(self):
        self.input_queue.start_consuming(self.process_data_messsage)

def main():
    logging.basicConfig(level=logging.INFO)
    sum_filter = SumFilter()
    sum_filter.start()
    return 0


if __name__ == "__main__":
    main()
