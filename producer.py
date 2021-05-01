from kafka import KafkaProducer
from kafka.errors import KafkaError
import cv2
import os
from multiprocessing import Pool
import time
import argparse
from string import Template
import random

# #kafka configuration
try:
    producer = KafkaProducer(
        bootstrap_servers=["server:9092"], max_request_size=3145728,
        request_timeout_ms=1000
    )
except KafkaError as e:
    print(e)

topic = "topic1"
topic_tempstr = Template("topic${thread}_${serial}")


def setting_capture(path):
    args = get_args()
    capture = cv2.VideoCapture(path, cv2.CAP_V4L2)
    capture.set(cv2.CAP_PROP_FOURCC, cv2.VideoWriter_fourcc(*"MJPG"))
    capture.set(cv2.CAP_PROP_FRAME_WIDTH, args.width)
    capture.set(cv2.CAP_PROP_FRAME_HEIGHT, args.height)
    capture.set(cv2.CAP_PROP_FPS, args.fps)
    return capture


def publish_video(tp):
    file = tp[0]
    serial = tp[1]
    topicname = tp[2]

    producer = KafkaProducer(
        bootstrap_servers=["localhost:9092"], max_request_size=3145728
    )
    capture = setting_capture(str(file))

    print("Process %s start !" % (serial))

    while capture.isOpened():
        _, frame = capture.read()
        # encode frame to jpeg then tobytes
        data = cv2.imencode(".jpeg", frame)[1].tobytes()
        future = producer.send(topicname, data)

        try:
            windows_name = "producer" + str(tp[1])
            cv2.namedWindow(windows_name, cv2.WINDOW_NORMAL)
            cv2.resizeWindow(windows_name, 200, 200)
            cv2.imshow(windows_name, frame)
            if cv2.waitKey(1) & 0xFF == ord("q"):
                break
            # metadata = future.get(timeout=1)
            # print(metadata.offset)
        except KafkaError as e:
            print(e)
            break

        if cv2.waitKey(1) == 27:
            capture.release()
            cv2.destroyAllWindows()
            print("------------stop------------")
            break

    capture.release()
    cv2.destroyAllWindows()


def multithread_publish(args):
    video_dir = "/media/cluster/0EA405370EA40537/video/"
    video_list = []
    for fname in os.listdir(video_dir):
        video_list.append(fname)
    if args.random:
        random.shuffle(video_list)

    topicstr = []
    for i in range(1, args.thread + 1):
        for j in range(1, args.topicperthread + 1):
            topicstr.append(topic_tempstr.substitute(thread=str(i), serial=str(j)))

    multithread_args = []
    for i in range(args.thread * args.topicperthread):
        multithread_args.append((video_list[i], i, topicstr[i]))
        print(multithread_args[i])

    pool = Pool(args.thread)
    res = pool.map(publish_video, multithread_args)
    pool.close()
    pool.join()

    print(res)


def measure(args):
    if args.camtest:
        print("image !!!")
        capture=setting_capture(0)
        list_time = []
        for i in range(100):
            ret, frame = capture.read()
            # encode frame to jpeg then tobytes
            data = cv2.imencode(".jpeg", frame)[1].tobytes()

            future = producer.send(topic, value=data)
            metadata = future.get(timeout=1)
            now_time = round(time.time() * 1000)
            send_time = metadata.timestamp
            # print("now time : %d " % (now_time))
            # print("send time : %d " % (send_time))
            list_time.append((now_time - send_time) / 2)
            # interval_time = (now_time-send_time)/2
            # print("interval time : %d" %(interval_time))
        interval_time = sum(list_time) / len(list_time)
        print("mean interval time : %.2f" % (interval_time))
    else:
        future = producer.send(topic, value=b"raw_bytes")
        metadata = future.get(timeout=1)
        now_time = round(time.time() * 1000)
        send_time = metadata.timestamp
        print("now time : %d " % (now_time))
        print("send time : %d " % (send_time))
        print("interval time : %d" % (now_time - send_time))


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--width", help="input width", default=1280, type=int)
    parser.add_argument("--height", help="input height", default=720, type=int)
    parser.add_argument("--fps", help="video frame rate", default=30, type=int)
    parser.add_argument("--thread", help="number of threads", default=8, type=int)
    parser.add_argument(
        "--camtest", help="use webcam to measure latency", action="store_true"
    )
    parser.add_argument("--random", help="to shuffle video list", action="store_true")
    parser.add_argument(
        "--topicperthread",
        help="the number of topic in thread to handle",
        default=1,
        type=int,
    )
    args = parser.parse_args()
    return args


if __name__ == "__main__":
    args = get_args()
    multithread_publish(args)
    # measure(args.camtest)
    pass
