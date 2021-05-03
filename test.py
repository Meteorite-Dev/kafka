from kafka import KafkaProducer
from kafka.errors import KafkaError
import cv2
from string import Template
import os
kafkaserver = Template("${serveraddr}:9092")
kafkastr = kafkaserver.substitute(serveraddr='server')
producer = KafkaProducer(bootstrap_servers=[kafkastr], max_request_size=10000000)
# # Asynchronous by default

# future = producer.send('topic1_1', b'raw_bytes')

# # Block for 'synchronous' sends
# try:
#     record_metadata = future.get(timeout=10)
# except KafkaError:
#     # Decide what to do if prod
#     pass

# # Successful result returns assigned partition and offset
# print (record_metadata.topic)
# print (record_metadata.partition)
# print (record_metadata.offset)
video_dir = "/media/cluster/0EA405370EA40537/video/"
video_list = []
for fname in os.listdir(video_dir):
    video_list.append(os.path.join(video_dir, fname))

capture = cv2.VideoCapture(video_list[1])
capture.set(cv2.CAP_PROP_FOURCC, cv2.VideoWriter_fourcc(*"MJPG"))
# capture.open(video_list[1], apiPreference=cv2.CAP_V4L2) #camera io important!!!
# capture.set(cv2.CAP_PROP_FRAME_WIDTH, 1280)
# capture.set(cv2.CAP_PROP_FRAME_HEIGHT, 720)
# capture.set(cv2.CAP_PROP_FPS, 10)
fps = capture.get(cv2.CAP_PROP_FPS)
print(fps)
while capture.isOpened():
    _,frame = capture.read()
    data = cv2.imencode(".jpeg", frame)[1].tobytes()
    future = producer.send("topic1_1",data)
    cv2.imshow("test", frame)
    if cv2.waitKey(1) & 0xFF == ord("q"):
        capture.release()
        cv2.destroyAllWindows()
        break
    record_metadata = future.get(timeout=1)
    print (record_metadata.offset)