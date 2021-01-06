import cv2
from kafka import KafkaConsumer
import numpy as np

# Fire up the Kafka Consumer
topic = "test-stream"

consumer = KafkaConsumer(
    topic, 
    bootstrap_servers=['localhost:9092'])

def get_video_stream():
    window_name="test"
    cv2.namedWindow(window_name, flags=cv2.WINDOW_FREERATIO)
    for msg in consumer:
        image_bytes = msg.value
        #decoded = cv2.imdecode(np.frombuffer(image_bytes, np.uint8), -1)
        decoded = cv2.imdecode(image_bytes, -1)
        # print(decoded.shape)
        # return decoded
        cv2.imshow(window_name, decoded)
        if cv2.waitKey(1) == 27:
            break
if __name__ == "__main__":
    print("consumer start")
    get_video_stream()