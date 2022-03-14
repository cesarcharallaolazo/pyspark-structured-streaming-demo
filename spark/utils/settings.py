import os

# Config Kafka
KAFKA_BROKERS = os.getenv(
    'kafka_brokers',
    'localhost:9092'
)

KAFKA_TOPIC_MODEL_PREDICTIONS = os.getenv(
    'modelpredictions_kafka_topic', 'topic1'
)

KAFKA_TOPIC_MODEL_FEATURES = os.getenv(
    'modelfeatures_kafka_topic', 'topic2'
)

# Config Local Database - Redis
DB_MICROSERVICE_REDIS_HOST = os.getenv('db_redis_host', 'localhost')
DB_MICROSERVICE_REDIS_PORT = int(os.getenv('db_redis_port', 6379))
