from kafbasic import Producer
import time
producer = Producer()
producer.producer_set(host=['node04:9092'] ,topics="testTopic")



while True :
    mes = input("message : ")

    res = producer.send(message=mes , react=True)
    print(res)