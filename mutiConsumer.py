from kafka import KafkaConsumer
import time
import cv2
import numpy as np
from multiprocessing import Process,Pool

consumer = KafkaConsumer( bootstrap_servers=['node04:9092'])


def recv(tp):
    print(tp[0])
    consumer = KafkaConsumer( bootstrap_servers=['node04:9092'])
    consumer.subscribe(str(tp[0]))

    num = 0
    avt = 0

    for msg in consumer:
        image_bytes = msg.value
        decoded = cv2.imdecode(np.frombuffer(image_bytes, np.uint8), -1)

        num +=1
        st = int(time.time()*1000) % 86400000
        rt = int(msg.key.decode('utf-8'))
        print(num)
        avt = ((st-rt) +avt)/num
        if num%500 ==0 :
            print("process " , tp[1] ," av : %.3f"%avt)


        cv2.imshow("rec", decoded)
        if cv2.waitKey(1) == 27:
            cv2.destroyAllWindows()
            print("stop")
            break


if __name__ =="__main__" :
    topics = ['thread0','thread1','thread2','thread3','thread4']
    # topic = ['testTopic']
    num = list(range(5))
    # print(num)
    tp = []
    for n in num:
        tp.append((topics[n] , n))
        print(tp[n])
    
    pool = Pool()

    pool.map(recv , tp)
    pool.close()
    pool.join()
    print("OK")

