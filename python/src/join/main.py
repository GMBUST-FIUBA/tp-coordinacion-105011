import os
import logging
import time
import signal
import heapq

from common import middleware, message_protocol, fruit_item

MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])
TOTAL_PARTIAL_TOPS_ACCEPTED = 3

class JoinFilter:

    def __init__(self):
        # Create input queue
        INPUT_QUEUE_PREFETCH_COUNT = 5
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE, prefetch_count=INPUT_QUEUE_PREFETCH_COUNT
        )

        # Create output queue
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )

        # Partial tops by client
        self.partial_tops_join_information = {}

        # Assign sigterm handler
        signal.signal(signalnum=signal.SIGTERM, handler=self._sigterm_handler)

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
                
                # Close data outputs
                self.output_queue.close()

                logging.info(f"Successful shutdown")
                break

            except:
                retry_time = self.__get_shutdown_retry_backoff(current_retries)
                time.sleep(retry_time)
                current_retries += 1

    def _send_fruits_top(self, sender_id):
        msg = [sender_id]

        # Add fruits top
        global_fruit_top_heap = self.partial_tops_join_information[sender_id]["heap"]
        fruits_top = heapq.nlargest(TOP_SIZE, global_fruit_top_heap)
        fruits_top_msg = [(item.fruit, item.amount) for item in fruits_top]
        msg.extend(fruits_top_msg)

        logging.info(f"Sending msg: {msg}")
        self.output_queue.send(message_protocol.internal.serialize(msg))

        logging.info(f"Erasing top...")
        self.partial_tops_join_information.pop(sender_id)

    def process_messsage(self, message, ack, nack):
        logging.info("Received top")
        fruit_top_msg = message_protocol.internal.deserialize(message)

        # Store in heap the partial tops
        (header, sender_id) = fruit_top_msg.pop()
        if header == "sender_id":
            # If there is no data yet from the client, add it
            # Otherwise, merge it with the other data gathered
            if sender_id not in self.partial_tops_join_information:
                new_client_join_info = {}
                self.partial_tops_join_information[sender_id] = new_client_join_info

                # Add heap
                fruit_top = [fruit_item.FruitItem(fruit_data_name, fruit_data_amount) for (fruit_data_name, fruit_data_amount) in fruit_top_msg]
                heapq.heapify_max(fruit_top)
                new_client_join_info["heap"] = fruit_top

                # Add total of lists received if there are more than one aggregators
                # Otherwise, send fruits top
                if AGGREGATION_AMOUNT == 1:
                    self._send_fruits_top(sender_id)

                new_client_join_info["total_results"] = 1
            else:
                client_join_info = self.partial_tops_join_information[sender_id]

                # Add new elements to top
                heap = client_join_info["heap"]
                for (fruit_name, fruit_amount) in fruit_top_msg:
                    new_fruit_item = fruit_item.FruitItem(fruit_name, fruit_amount)
                    heapq.heappush_max(heap, new_fruit_item)

                # Add to total of messages received
                client_join_info["total_results"] += 1

                if client_join_info["total_results"] == AGGREGATION_AMOUNT:
                    self._send_fruits_top(sender_id)
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
