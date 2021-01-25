from kafka import KafkaConsumer
import time
import cv2
import numpy as np


# topics = ['thread0','thread1','thread2','thread3','thread4']
topic = ['testTopic']

consumer = KafkaConsumer( bootstrap_servers=['node04:9092'])
consumer.subscribe(topic)

num = 0
avt = 0

for msg in consumer:
    image_bytes = msg.value
    decoded = cv2.imdecode(np.frombuffer(image_bytes, np.uint8), -1)

    num +=1
    st = int(time.time()*1000) % 86400000
    rt = int(msg.key.decode('utf-8'))
    # print(st -rt)
    avt = ((st-rt) +avt)/num
    print("av : %.3f"%avt)


    cv2.imshow("rec", decoded)
    if cv2.waitKey(1) == 27:
        cv2.destroyAllWindows()
        print("stop")
        break


