from kafka import KafkaProducer
import time
import jsonlines
import json

producer = KafkaProducer(bootstrap_servers=['localhost:9092'] ,value_serializer=lambda v: json.dumps(v).encode('utf-8') )

# while True :
#     msg = input("msg : ")
#     send_time = time.time()
#     send_time = str(int((send_time *1000) % 86400000)).encode('utf-8')
#     future = producer.send('testTopic' ,value=msg , key = send_time, partition=0)
#     result = future.get(timeout=10)
#     print(result)
with open('test.json', 'r') as js :
    data = json.load(js)
send_time = time.time()
send_time = str(int((send_time *1000) % 86400000)).encode('utf-8')
fut = producer.send('testTopic' , value=data, key=send_time)
res = fut.get(timeout = 10)
print(res)