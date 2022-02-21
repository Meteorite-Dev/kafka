from ckafka import cKafka_Producer
import argparse
import cv2
from time import sleep


def get_parser():
    parser = argparse.ArgumentParser(description="Simg")
    parser.add_argument("--image", nargs='?')
    parser.add_argument("--video", nargs='?')
    parser.add_argument("--topic" ,default="testTopic" ,nargs=1)

    return parser

def _frame_from_video(video):
    while video.isOpened():
        success, frame = video.read()
        if success:
            yield frame
        else:
            break

if __name__ == "__main__":
    producer = cKafka_Producer(server_ip="localhost" ,port=9094 ,topics='testTopic')
    args = get_parser().parse_args()
    
    if args.image:
        image = cv2.imread(args.image[0])

        producer.image_Producer(message=image ,topic='testTopic' , t=True)
        sleep(3)
    elif args.video:
        video = cv2.VideoCapture(args.video_input)
        width = int(video.get(cv2.CAP_PROP_FRAME_WIDTH))
        height = int(video.get(cv2.CAP_PROP_FRAME_HEIGHT))
        frames_per_second = video.get(cv2.CAP_PROP_FPS)
        frame = _frame_from_video(video)
        for cnt, frame in enumerate(frame):
            producer.image_Producer(message=frame , topics=args.topic[0], t=True)
    else:
        print("need image or video args.")
        raise 
    

