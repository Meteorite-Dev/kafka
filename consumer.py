from kafka import KafkaConsumer

topics = ['thread0','thread1','thread2','thread3','thread4']
topic = ['testTopic']

consumer = KafkaConsumer( bootstrap_servers=['localhost:9092'], value_deserializer = lambda v : v.decode('utf-8'))
consumer.subscribe(topics)

for msg in consumer:
    print(msg)