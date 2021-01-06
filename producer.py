from kafbasic import Producer

producer = Producer()
producer.producer_set(host=['localhost:9092'] ,topics="testTopic")


while True :
    mes = input("message : ")

    res = producer.send(message=mes , react=True)
    print(res)