from kafka import KafkaConsumer
import time

topics = ['thread0','thread1','thread2','thread3','thread4']
topic = ['testTopic']

consumer = KafkaConsumer( bootstrap_servers=['localhost:9092'], value_deserializer = lambda v : v.decode('utf-8'))
consumer.subscribe(topic)

num = 0
avt = 0

for msg in consumer:
    num +=1
    st = int(time.time()*1000) % 86400000
    rt = int(msg.value)
    # print(st -rt)
    avt = ((st-rt) +avt)/num
    print("av : %.3f"%avt)
