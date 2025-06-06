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
