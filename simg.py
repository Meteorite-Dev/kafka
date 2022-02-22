from ckafka import cKafka_Producer
import argparse
import cv2
from time import sleep


def get_parser():
    parser = argparse.ArgumentParser(description="Simg")
    parser.add_argument("--image", nargs='?')
    parser.add_argument("--video", nargs='?')
    parser.add_argument("--ip",nargs="+",help="kafka server ip." ,default="localhost" ,type=str)
    parser.add_argument("--port",nargs="+",help="kafka server port." ,default="9094" ,type=str)
    parser.add_argument("--intopic" ,default="image" ,nargs=1)
    parser.add_argument("--outtopic" ,default="image" ,nargs=1)
    return parser

def _frame_from_video(video):
    while video.isOpened():
        success, frame = video.read()
        if success:
            yield frame
        else:
            break

if __name__ == "__main__":
    args = get_parser().parse_args()
    producer = cKafka_Producer(server_ip=args.ip[0] ,port=args.port[0] ,topics=args.outtopic[0])

    if args.image:
        image = cv2.imread(args.image)

        producer.image_Producer(message=image, t=True)
        sleep(3)
    elif args.video:
        video = cv2.VideoCapture(args.video)
        # width = int(video.get(cv2.CAP_PROP_FRAME_WIDTH))
        # height = int(video.get(cv2.CAP_PROP_FRAME_HEIGHT))
        frames_per_second = video.get(cv2.CAP_PROP_FPS)
        frame = _frame_from_video(video)
        for cnt, frame in enumerate(frame):
            producer.image_Producer(message=frame, t=True)
    else:
        print("need image or video args.")
        raise 
    

