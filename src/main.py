import argparse
import os
from queue import Queue
import sys
import threading

from consumer import Consumer
from producer import Producer


def parse_arguments():
    parser = argparse.ArgumentParser(description="Fetch and parse URLs via Producer/Consumer.")
    parser.add_argument("-f", "--file", type=str, help="Path to input file containing URLs.")
    parser.add_argument("-t", "--threads", type=int, default=4, help="Number of worker fetch threads.")
    parser.add_argument("-s", "--selector", type=str, default=None, help="CSS selector to filter links nested under matching elements.")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_arguments()

    # Graceful check for missing input file
    if args.file and not os.path.isfile(args.file):
        sys.stderr.write(f"Error: Input file '{args.file}' not found.\n")
        sys.exit(1)

    shared_queue = Queue()

    producer = Producer(out_queue=shared_queue, num_threads=args.threads)
    consumer = Consumer(
        in_queue=shared_queue,
        out_stream=sys.stdout,
        selector=args.selector,
    )

    def producer_worker():
        if args.file:
            producer.process_file(args.file)
        else:
            raw_input = sys.stdin.read()
            producer.process_string(raw_input)

    producer_thread = threading.Thread(target=producer_worker, name="ProducerThread")
    consumer_thread = threading.Thread(target=consumer.run, name="ConsumerThread")

    producer_thread.start()
    consumer_thread.start()

    producer_thread.join()
    consumer_thread.join()