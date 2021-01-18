from kafbasic import Producer
import time

producer = Producer()
producer.producer_set(host=['localhost:9092'] )



while True :
    st = int(time.time()*1000) % 86400000
    mes = str(st)
    print(mes)
    producer.send(topics="testTopic" ,message=mes, react=False)
