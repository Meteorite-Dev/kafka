from kafka import consumer
from kafbasic import Consumer

consumer = Consumer()
consumer.consumer_set(host=['localhost:9092'], topics="testTopic" )

consumer.listen(tp="ms")
