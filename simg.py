from ckafka import cKafka_Producer

import cv2

from time import sleep

producer = cKafka_Producer(server_ip="localhost" ,port=9094 ,topics='testTopic')

image = cv2.imread("../0001.jpg")

producer.image_Producer(message=image ,topic='testTopic' , t=True)
sleep(5)

