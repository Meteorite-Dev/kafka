# from kafka import KafkaConsumer

# topics = ['thread0','thread1','thread2','thread3','thread4']
# topic = ['testTopic']

# consumer = KafkaConsumer( bootstrap_servers=['node04:9092'], value_deserializer = lambda v : v.decode('utf-8'))
# consumer.subscribe(topic)

# for msg in consumer:
#     print(msg)

from kafbasic import Consumer

host = ['node04:9092']
topics= ['testTopic']

consumer = Consumer()
consumer.consumer_set(host=host , topics=topics ,wait=False)
consumer.listen()