from kafka import KafkaProducer
from kafka.errors import KafkaError
import random
from crawler import logs


# KAFKA PRODUCER #####
def kafkaProduc(topic, vkey, vvalue, vfile):
    topic_partitions = [0,1]
    partition = random.choice(topic_partitions)
    try:
        producer = KafkaProducer(bootstrap_servers=['jarvis2:9092'], retries=5)
        future = producer.send(topic, key=str.encode(str(vkey)), value=str.encode(str(vvalue)), partition=partition)
        record_metadata = future.get(timeout=10)

    except KafkaError as nm:
        logs.kafkaLog('ERROR: ' + str(vfile), str(nm), 'err')
        pass

    return