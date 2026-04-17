import os
import logging
import time

from common import middleware, message_protocol, fruit_item

MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])


class JoinFilter:

    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )

    # Sigterm handler
    def _sigterm_handler(self, signum, frame):
        self.shutdown()

    def __get_shutdown_retry_backoff(self, current_retries):
        RETRY_SHUT_DOWN_TIME_SEC = 0.1
        return RETRY_SHUT_DOWN_TIME_SEC

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


    def process_messsage(self, message, ack, nack):
        logging.info("Received top")
        fruit_top_msg = message_protocol.internal.deserialize(message)

        # Send as many messages as clients there are
        (header, sender_id) = fruit_top_msg.pop()
        if header == "sender_id":
            msg = [sender_id]
            msg.extend(fruit_top_msg)
            logging.info(f"Sending msg: {msg}")
            self.output_queue.send(message_protocol.internal.serialize(msg))
            ack()
        else:
            nack()

    def start(self):
        self.input_queue.start_consuming(self.process_messsage)


def main():
    logging.basicConfig(level=logging.INFO)
    join_filter = JoinFilter()
    join_filter.start()

    return 0


if __name__ == "__main__":
    main()
