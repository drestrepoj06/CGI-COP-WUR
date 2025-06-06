from kafka import KafkaAdminClient, KafkaProducer
from kafka.admin import NewTopic
from kafka.errors import KafkaError, TopicAlreadyExistsError
import time
import sys

# Kafka 配置
KAFKA_BROKER = 'kafka:9092'
TOPICS = ['orders', 'notifications', 'events']

def create_kafka_admin():
    """创建 Kafka 管理员客户端（带重试机制）"""
    max_retries = 5
    for i in range(max_retries):
        try:
            admin = KafkaAdminClient(
                bootstrap_servers=[KAFKA_BROKER],
                api_version=(3, 6, 0)
            )
            print(f"✅ 成功连接 Kafka Broker: {KAFKA_BROKER}")
            return admin
        except Exception as e:
            if i == max_retries - 1:
                print(f"❌ 创建 KafkaAdminClient 失败: {e}", file=sys.stderr)
                sys.exit(1)
            print(f"⚠️ 连接失败 ({i+1}/{max_retries})，5秒后重试...")
            time.sleep(5)

def create_topics(admin_client):
    """创建主题列表"""
    print("⏳ 正在创建 Kafka 主题...")
    topic_list = [NewTopic(topic, num_partitions=1, replication_factor=1) for topic in TOPICS]
    
    try:
        admin_client.create_topics(new_topics=topic_list, validate_only=False)
        print(f"✅ 成功创建 {len(TOPICS)} 个主题: {', '.join(TOPICS)}")
    except TopicAlreadyExistsError:
        print(f"ℹ️ 主题已存在，跳过创建: {', '.join(TOPICS)}")
    except Exception as e:
        print(f"❌ 创建主题失败: {e}", file=sys.stderr)
        sys.exit(1)

def produce_messages():
    """生产消息到所有主题"""
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        api_version=(3, 6, 0)
    )
    
    for topic in TOPICS:
        try:
            for i in range(3):  # 每个主题发3条消息
                message = f"测试消息 {i} - {time.strftime('%H:%M:%S')}".encode('utf-8')
                producer.send(topic, message)
                producer.flush()
                print(f"📤 发送到 [{topic}]: {message.decode()}")
        except KafkaError as e:
            print(f"❌ 生产者错误 ({topic}): {e}", file=sys.stderr)
    
    producer.close()

if __name__ == "__main__":
    try:
        admin_client = create_kafka_admin()
        create_topics(admin_client)
        admin_client.close()
        produce_messages()
        print("🎉 全部操作完成！")
    except KeyboardInterrupt:
        print("🛑 程序被中断")
        sys.exit(0)



import json
import time
import logging
import requests
import threading
import sys
from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import KafkaError, TopicAlreadyExistsError
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

# Kafka configuration
KAFKA_BROKER = 'kafka:9092'
TOPICS = ['train-locations', 'sim-tomtom-vehicle']  # Add other topics as needed

# --- Kafka Admin Functions ---

def create_kafka_admin(max_retries=5, retry_interval=5):
    """创建 Kafka 管理员客户端（带重试机制）"""
    for i in range(max_retries):
        try:
            admin = KafkaAdminClient(
                bootstrap_servers=[KAFKA_BROKER],
                api_version=(3, 0, 0)  # Match your producer API version
            )
            logging.info(f"✅ Successfully connected to Kafka Broker: {KAFKA_BROKER}")
            return admin
        except Exception as e:
            if i == max_retries - 1:
                logging.error(f"❌ Failed to create KafkaAdminClient: {e}")
                raise
            logging.warning(f"⚠️ Connection failed ({i+1}/{max_retries}), retrying in {retry_interval}s...")
            time.sleep(retry_interval)

def ensure_topics_exist(admin_client=None):
    """Ensure required Kafka topics exist"""
    try:
        if admin_client is None:
            admin_client = create_kafka_admin()
            
        existing_topics = admin_client.list_topics()
        topics_to_create = [t for t in TOPICS if t not in existing_topics]
        
        if topics_to_create:
            topic_list = [NewTopic(topic, num_partitions=1, replication_factor=1) for topic in topics_to_create]
            admin_client.create_topics(new_topics=topic_list, validate_only=False)
            logging.info(f"✅ Created {len(topic_list)} topics: {', '.join(topics_to_create)}")
        else:
            logging.info(f"ℹ️ All topics already exist: {', '.join(TOPICS)}")
            
        return admin_client
    except TopicAlreadyExistsError:
        logging.info(f"ℹ️ Topics already exist: {', '.join(TOPICS)}")
    except Exception as e:
        logging.error(f"❌ Failed to create topics: {e}")
        raise
    finally:
        if admin_client:
            admin_client.close()

# --- Kafka Producer Functions ---

def create_kafka_producer():
    """创建 Kafka 生产者实例"""
    return KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        api_version=(3, 0, 0),
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )

# --- Original Producer Functions (unchanged) ---

TRAIN_LOGS_PATH = 'utils/train_logs.json'

def load_train_logs():
    with open(TRAIN_LOGS_PATH, 'r') as f:
        return json.load(f)

def parse_timestamp(ts_str):
    dt = datetime.fromisoformat(ts_str)
    return int(dt.timestamp() * 1000)

def produce_train_messages(producer):
    try:
        while True:
            train_logs = load_train_logs()
            for entry in train_logs:
                timestamp = parse_timestamp(entry['timestamp'])
                for train in entry['trains']:
                    message = {"key": "value"}  # Your message format
                    logging.info(f"Sending train data: {message}")
                    producer.send('train-locations', message)
                    logging.info("'train-locations' message sent successfully!")
                producer.flush()
                time.sleep(5)
    except Exception as e:
        logging.error(f"Train producer error: {e}")


def produce_vehicle_messages():
    destination = (51.9, 4.5)  # Example fixed destination
    try:
        while True:
            for vehicle_id, vehicle_data in vehicles_config.items():
                origin = vehicle_data['location'][0]
                route = get_route_from_tomtom(origin, destination, traffic=True)
                if route:
                    message = {"key": "value"}
                    # message = {
                    #     "timestamp": int(time.time() * 1000),
                    #     "type": "vehicle",
                    #     "vehicle_id": vehicle_id,
                    #     "mode": vehicle_data['mode'],
                    #     "speed": vehicle_data['speed'],
                    #     "origin": origin,
                    #     "destination": destination,
                    #     "route_summary": {
                    #         "lengthInMeters": route["lengthInMeters"],
                    #         "travelTimeInSeconds": route["travelTimeInSeconds"],
                    #         "trafficDelayInSeconds": route["trafficDelayInSeconds"],
                    #     },
                    #     "route_geometry": route["geometry"]
                    # }

                    logging.info(f"Sending vehicle route data: {message}")
                    producer.send('sim-tomtom-vehicle', message)
                    logging.info("sim-tomtom-vehicle' message sent successfully!")
                else:
                    logging.warning(f"Skipping message for {vehicle_id} due to route fetch failure.")
            producer.flush()
            time.sleep(2)  # Wait 2 seconds before next batch
    except Exception as e:
        logging.error(f"Vehicle producer error: {e}")


# --- Main Function ---

def main():
    try:
        # 1. Ensure Kafka topics exist
        ensure_topics_exist()
        
        # 2. Create producer
        producer = create_kafka_producer()
        
        # 3. Start producer threads
        train_thread = threading.Thread(
            target=produce_train_messages,
            args=(producer,),
            daemon=True
        )
        
        vehicle_thread = threading.Thread(
            target=produce_vehicle_messages,
            args=(producer,),
            daemon=True
        )
        
        train_thread.start()
        vehicle_thread.start()
        
        # Keep main thread alive
        while True:
            time.sleep(1)
            
    except KeyboardInterrupt:
        logging.info("Stopping producer...")
    except Exception as e:
        logging.error(f"Fatal error: {e}")
        sys.exit(1)
    finally:
        if 'producer' in locals():
            producer.close()
        logging.info("Application shutdown complete")

if __name__ == "__main__":
    main()

