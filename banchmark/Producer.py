from kafka import KafkaProducer
import time

producer = KafkaProducer(bootstrap_servers=['localhost:9092'] , value_serializer = lambda x : x.encode('utf-8') )

while True :
    msg = input("msg : ")
    send_time = time.time()
    send_time = str(int((send_time *1000) % 86400000)).encode('utf-8')
    future = producer.send('testTopic' ,value=msg , key = send_time, partition=0)
    result = future.get(timeout=10)
    print(result)
