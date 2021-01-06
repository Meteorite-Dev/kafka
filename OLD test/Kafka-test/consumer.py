from kafka import KafkaConsumer

wating_time_ms = 10000

consumer = KafkaConsumer('testTopic',
                         bootstrap_servers=['localhost:9092'],  consumer_timeout_ms=wating_time_ms)
for msg in consumer:
    # print(type(msg.value))
    message = msg.value.decode()
    # print(type(message))
    print(message)
    print(msg.key)
