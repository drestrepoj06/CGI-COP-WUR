import os
import logging
from multiprocessing import Process
from producer import main as producer_main
from consumer import main as consumer_main

def setup_logging():
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format='%(asctime)s - %(processName)s - %(levelname)s - %(message)s'
    )

def run_producer():
    logging.info("Producer process started")
    producer_main()

def run_consumer():
    logging.info("Consumer process started")
    consumer_main()

if __name__ == "__main__":
    setup_logging()
    
    # Create processes
    producer = Process(target=run_producer, name="Producer")
    consumer = Process(target=run_consumer, name="Consumer")

    try:
        # Start processes
        producer.start()
        consumer.start()
        logging.info("Both producer and consumer started")

        # Keep main process alive
        while True:
            if not producer.is_alive():
                logging.error("Producer died! Restarting...")
                producer = Process(target=run_producer, name="Producer")
                producer.start()
                
            if not consumer.is_alive():
                logging.error("Consumer died! Restarting...")
                consumer = Process(target=run_consumer, name="Consumer")
                consumer.start()

            producer.join(timeout=1)
            consumer.join(timeout=1)

    except KeyboardInterrupt:
        logging.info("Shutting down gracefully...")
        producer.terminate()
        consumer.terminate()
    finally:
        producer.join()
        consumer.join()

