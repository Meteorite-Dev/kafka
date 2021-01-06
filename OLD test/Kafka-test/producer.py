from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
# future = producer.send('testTopic', key=b'my_key',
#                        value=b'my_value', partition=0)
# result = future.get(timeout=10)
# print(result)

while True :
    msg = input("msg : ")
    msgb = str.encode(msg)
    future = producer.send('testTopic' ,value=msgb , partition=0)
    result = future.get(timeout=10)
    print(result)
