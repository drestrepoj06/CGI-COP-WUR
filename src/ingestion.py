import threading
import time
import logging

# Import the main functions from consumer.py and producer.py
from consumer import main as consumer_main
from producer import main as producer_main


def run_producer():
    """Run the Kafka producer process."""
    try:
        producer_main()
    except Exception as e:
        logging.error(f"Producer encountered an error: {e}")


def run_consumer():
    """Run the Kafka consumer process."""
    try:
        consumer_main()
    except Exception as e:
        logging.error(f"Consumer encountered an error: {e}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s %(levelname)s %(message)s')

    # Create threads for producer and consumer
    producer_thread = threading.Thread(
        target=run_producer, daemon=True, name="ProducerThread")
    consumer_thread = threading.Thread(
        target=run_consumer, daemon=True, name="ConsumerThread")

    # Start both threads
    producer_thread.start()
    consumer_thread.start()

    logging.info(
        "Producer and Consumer threads started. Press Ctrl+C to exit.")

    # Keep the main thread running to allow background threads to do their work
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logging.info("Exiting application...")
