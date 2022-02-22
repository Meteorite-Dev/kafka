from ckafka import cKafka_Consumer
import cv2
from time import time

consumer = cKafka_Consumer(server_ip="localhost",
                           port=9094, topics=['t'])
# print(consumer.topics)
print("recv time")
for mes in consumer.image_Consumer(t=True):
    st = mes[1][1]
    rt = mes[2]
    print((rt-st))
    # cv2.imshow("show rimage." ,mes)
    # if cv2.waitKey(0) == 27:
    #     break  # esc to quit
