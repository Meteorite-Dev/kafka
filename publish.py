import sys
import time
import cv2
from kafka import KafkaProducer

topic = "test-stream"

def publish_video(video_file):
    # Start up producer
    producer = KafkaProducer(bootstrap_servers='localhost:9092')
    # Open file
    video = cv2.VideoCapture(video_file)
    print('publishing video...')
    while(video.isOpened()):
        success, frame = video.read()
        # Ensure file was read successfully
        if not success:
            print("bad read!")
            break
        # Convert image to png
        ret, buffer = cv2.imencode('.jpg', frame)
        # Convert to bytes and send to kafka
        producer.send(topic, buffer.tobytes())
        time.sleep(0.01)
    video.release()
    print('publish complete')

def publish_camera():
    # Start up producer
    producer = KafkaProducer(bootstrap_servers='localhost:9092')
    # camera = cv2.VideoCapture("rtsp://140.132.48.228")
    camera = cv2.VideoCapture(0)
    # try:
    while(True):
        frame = camera.read()[1]
        buffer = cv2.imencode('.jpg', frame)[1]
        producer.send(topic, buffer.tobytes())

            # Choppier stream, reduced load on processor
        time.sleep(0.02)
    # except:
    #     print("\nExiting.")
    #     sys.exit(1)
    camera.release()

if __name__ == '__main__':

    """
    Producer will publish to Kafka Server a video file given as a system arg. 
    Otherwise it will default by streaming webcam feed.
    """
    if(len(sys.argv) > 1):
        video_path = sys.argv[1]
        publish_video(video_path)
    else:
        print("publishing feed!")
        publish_camera()
