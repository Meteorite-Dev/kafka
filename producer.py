from kafka import KafkaProducer
from kafka.errors import KafkaError
import cv2
import sys
from multiprocessing import Pool
import time
import argparse

# #kafka configuration
producer = KafkaProducer(bootstrap_servers=["server:9092"], max_request_size=3145728)
topic = "topic1"
# # camera configuration
# cam1 = cv2.VideoCapture()
# cam1.open(0, apiPreference=cv2.CAP_V4L2) #video io important!!!
# cam1.set(cv2.CAP_PROP_FOURCC, cv2.VideoWriter_fourcc(*"MJPG"))
# cam1.set(cv2.CAP_PROP_FRAME_WIDTH, 1280)
# cam1.set(cv2.CAP_PROP_FRAME_HEIGHT, 720)
# cam1.set(cv2.CAP_PROP_FPS, 30)


def cam(tp):
    producer = KafkaProducer(
        bootstrap_servers=["localhost:9092"], max_request_size=3145728
    )
    capture = cv2.VideoCapture(str(tp[0]))
    capture.set(cv2.CAP_PROP_FOURCC, cv2.VideoWriter_fourcc(*"MJPG"))
    capture.set(cv2.CAP_PROP_FRAME_WIDTH, 1280)
    capture.set(cv2.CAP_PROP_FRAME_HEIGHT, 720)
    capture.set(cv2.CAP_PROP_FPS, 30)

    print("Process %s start !" % (tp[1]))

    while capture.isOpened():
        _, frame = capture.read()
        # encode frame to jpeg then tobytes
        data = cv2.imencode(".jpeg", frame)[1].tobytes()
        future = producer.send(str(tp[2]), data)

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


def multithread_publish():
    # fi =sys.argv[0]
    # linum =sys.argv[1]
    linum = 8
    base_dir = "/media/cluster/0EA405370EA40537/"
    path = [
        base_dir + "video/2020-10-20_12:00:01.avi",
        base_dir + "video/2020-10-16_12:00:01.avi",
        base_dir + "video/2020-10-16_11:00:01.avi",
        base_dir + "video/2020-10-19_15:50:01.avi",
        base_dir + "video/2020-10-20_15:50:01.avi",
        base_dir + "video/2020-10-19_07:40:01.avi",
        base_dir + "video/2020-10-19_09:50:01.avi",
        base_dir + "video/2020-10-21_09:50:01.avi",
        base_dir + "video/2020-10-21_11:00:01.avi",
        base_dir + "video/2020-10-21_12:00:01.avi",
    ]
    num = list(range(int(linum)))
    # print(num)
    topics = [
        "thread0",
        "thread1",
        "thread2",
        "thread3",
        "thread4",
        "thread5",
        "thread6",
        "thread7",
        "thread8",
        "thread9",
    ]
    tp = []
    for n in num:
        # tp[0] path tp[1] serial no tp[2] topic
        tp.append((path[n], n, topics[n]))
        print(tp[n])

    pool = Pool(8)
    # cap = partial(cap_video , num=range(5))

    res = pool.map(cam, tp)
    pool.close()
    pool.join()
    print(res)


def measure(cam=False):
    if cam:
        print("image !!!")
        capture = cv2.VideoCapture(0)
        capture.set(cv2.CAP_PROP_FOURCC, cv2.VideoWriter_fourcc(*"MJPG"))
        capture.set(cv2.CAP_PROP_FRAME_WIDTH, 1280)
        capture.set(cv2.CAP_PROP_FRAME_HEIGHT, 720)
        capture.set(cv2.CAP_PROP_FPS, 30)
        list_time = []
        for i in range(100):
            # print(i)
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


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("-w", "--width", help="input width", default=1280, type=int)
    parser.add_argument("-H", "--height", help="input height", default=720, type=int)
    parser.add_argument("-f", "--fps", help="video frame rate", default=30, type=int)
    parser.add_argument("-t", "--thread", help="number of threads", default=8, type=int)
    args = parser.parse_args()
    return args


if __name__ == "__main__":
    # multithread_publish()
    args = parse_args()
    # measure(True)
    pass
