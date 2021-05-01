from kafka import KafkaConsumer
import cv2
import numpy as np
from multiprocessing import Process,Pool
# To consume latest messages and auto-commit offsets
topic = 'topic1'
consumer = KafkaConsumer(topic,
                         bootstrap_servers=['localhost:9092'])

for message in consumer:
    img_byte = message.value
    img_decode = cv2.imdecode(np.frombuffer(img_byte,np.uint8),-1)
    # print(img_decode.shape)
    cv2.imshow('consumer', img_decode)
    if cv2.waitKey(1) & 0xFF == ord('q'):
        break
    # message value and key are raw bytes -- decode if necessary!
    # e.g., for unicode: `message.value.decode('utf-8')`
    # print ("%s:%d: key=%s value=%s time=%d" % (message.topic,
                                        #   message.offset, 
                                        #   message.key,
                                        #   message.value
                                        #   message.timestamp%86400))

    # print("time=%d" % (message.timestamp))
