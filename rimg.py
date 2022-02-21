import imp
from ckafka import cKafka_Consumer
import cv2


consumer = cKafka_Consumer(server_ip="localhost",
                           port=9094, topics='testTopic')

for mes in consumer.image_Consumer():
    cv2.imshow("show rimage." ,mes)
    if cv2.waitKey(0) == 27:
        break  # esc to quit
