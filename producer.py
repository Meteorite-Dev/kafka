# from kafbasic import Producer
import time
import cv2 
from kafka import KafkaProducer

# ser = lambda V:V.tobytes()

# producer = Producer()
# producer.producer_set(host=['node04:9092'] ,serializer=ser)
producer = KafkaProducer(bootstrap_servers='node04:9092',max_request_size=32000000)

capture = cv2.VideoCapture(0)
capture.set(cv2.CAP_PROP_FOURCC, cv2.VideoWriter_fourcc('M', 'J', 'P', 'G'))
print('ipcam started!')

while True :
    # print(status)
    status, Frame = capture.read()
    buffer = cv2.imencode('.jpg', Frame)[1]
    
    res , buffer = cv2.imencode('.jpg', Frame)
    st = int(time.time()*1000) % 86400000
    cv2.imshow('Image', Frame)
    mes = str(st)
    print(mes)
    producer.send("testTopic" ,value=buffer.tobytes() , key=mes.encode('utf-8'))
    if cv2.waitKey(1) == 27:
        cv2.destroyAllWindows()
        print("stop")
        break
    time.sleep(0.02)


# while True :
#     st = int(time.time()*1000) % 86400000
#     mes = str(st)
#     print(mes)
#     producer.send(topics="testTopic" ,message=mes, react=False)
