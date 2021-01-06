from kafka import KafkaConsumer
import time

wating_time_ms = 10000

consumer = KafkaConsumer('testTopic',
                         bootstrap_servers=['localhost:9092'],
                         value_deserializer=lambda x :x.decode('utf-8')
                         )
for msg in consumer:
    rec_time = time.time()
    rec_time = int((rec_time * 1000) % 86400000)
    message = msg.value
    send_time = msg.key
    send_time = int(send_time.decode('utf-8'))
    print(type(message))
    print(message)
    print(rec_time - send_time)
