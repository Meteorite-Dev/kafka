# from kafka import KafkaProducer

# producer = KafkaProducer(bootstrap_servers=['172.31.37.71:9092'])
# i=0
# while True :
#     msgb = "test"
#     future = producer.send('testTopic' ,value=msgb , partition=0)
#     result = future.get(timeout=10)
#     print(result)
from kafka import KafkaProducer
from kafka.errors import KafkaError

producer = KafkaProducer(bootstrap_servers=['172.31.37.71:9092'])

# asynchronous by default
future = producer.send('my-test-topic', b'raw_bytes')
print(future)

# block for 'synchronous' sends
try:
    record_metadata = future.get(timeout=10)
    print(record_metadata)
except KafkaError:
    # decide what to do if produce request failed...
    pass

# successful result returns assigned partition and offset
print(record_metadata.topic)
print(record_metadata.partition)
print(record_metadata.offset)