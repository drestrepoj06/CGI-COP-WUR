import json
import logging
import sys
import time
from kafka import KafkaConsumer

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

# Kafka configuration
KAFKA_BROKER = 'kafka:9092'
TOPIC = 'train-locations'

def create_kafka_consumer():
    """
    Create a Kafka consumer instance.
    This consumer will listen to the 'train-locations' topic,
    deserialize incoming messages from JSON, and use the earliest offset.
    """
    try:
        consumer = KafkaConsumer(
            TOPIC,
            bootstrap_servers=[KAFKA_BROKER],
            auto_offset_reset='earliest',   # Start reading at the earliest message
            enable_auto_commit=True,
            group_id='train-location-consumers',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        logging.info(f"✅ Successfully connected to Kafka Broker: {KAFKA_BROKER} and subscribed to topic: {TOPIC}")
        return consumer
    except Exception as e:
        logging.error(f"❌ Failed to create Kafka consumer: {e}")
        sys.exit(1)

def main():
    consumer = create_kafka_consumer()
    
    try:
        # Consume messages indefinitely
        for message in consumer:
            # message.value will be a dictionary because of the deserializer
            logging.info(f"📥 Received message from '{TOPIC}': {message.value}")
    except KeyboardInterrupt:
        logging.info("Interrupt received, shutting down consumer...")
    except Exception as e:
        logging.error(f"Error processing message: {e}")
    finally:
        consumer.close()
        logging.info("Consumer shutdown complete.")

if __name__ == "__main__":
    main()


