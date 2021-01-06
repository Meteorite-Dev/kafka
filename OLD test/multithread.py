"""
source ： https://zhuanlan.zhihu.com/p/38136322
github : https://github.com/Yonv1943/Python/blob/master/Demo_camera_and_network/ip_camera.py
"""
import time
import multiprocessing as mp
import cv2
import numpy as np
from kafka import KafkaProducer
# 理學門口
URL = "rtsp://140.132.48.228"
topic = "test-stream"
# Start up producer
producer = KafkaProducer(bootstrap_servers='localhost:9092')

def image_put(q):
    cap = cv2.VideoCapture(URL)
    if cap.isOpened():
        print('open success')
    while True:
        q.put(cap.read()[1])
        # q.get() if q.qsize() > 1 else time.sleep(0.01)


def image_get(q, window_name):
    cv2.namedWindow(window_name, flags=cv2.WINDOW_FREERATIO)
    while True:
        frame = q.get()
        cv2.imshow(window_name, frame)
        if cv2.waitKey(1) == 27:
            cv2.destroyAllWindows()
            break

def image_publish(q):
    while True:
        frame = q.get()
        ret, buffer = cv2.imencode('.jpg', frame)
        producer.send(topic, buffer.tobytes())
        time.sleep(0.01)

def run_single_camera():
    # user_name, user_pwd, camera_ip = "admin", "admin123456", "172.20.114.196"
    # user_name, user_pwd, camera_ip = "admin", "admin123456", "[fe80::3aaf:29ff:fed3:d260]"
    camera_ip = "test1"
    mp.set_start_method(method='fork')  # init
    queue = mp.Queue(maxsize=2)
    # processes = [mp.Process(target=image_put, args=(queue,)),
    #              mp.Process(target=image_get, args=(queue, camera_ip))]
    processes = [mp.Process(target=image_put, args=(queue,)),
                 mp.Process(target=image_publish, args=(queue,))]

    [process.start() for process in processes]
    [process.join() for process in processes]


def run_multi_camera():
    user_name, user_pwd = "admin", "admin123456"
    camera_ip_l = [
        "172.20.114.196",  # ipv4
        "[fe80::3aaf:29ff:fed3:d260]",  # ipv6
    ]

    mp.set_start_method(method='spawn')  # init
    queues = [mp.Queue(maxsize=4) for _ in camera_ip_l]

    processes = []
    for queue, camera_ip in zip(queues, camera_ip_l):
        processes.append(mp.Process(target=image_put, args=(queue, user_name, user_pwd, camera_ip)))
        processes.append(mp.Process(target=image_get, args=(queue, camera_ip)))

    for process in processes:
        process.daemon = True
        process.start()
    for process in processes:
        process.join()


def image_collect(queue_list, camera_ip_l):
    import numpy as np

    """show in single opencv-imshow window"""
    window_name = "%s_and_so_no" % camera_ip_l[0]
    cv2.namedWindow(window_name, flags=cv2.WINDOW_FREERATIO)
    while True:
        imgs = [q.get() for q in queue_list]
        imgs = np.concatenate(imgs, axis=1)
        cv2.imshow(window_name, imgs)
        cv2.waitKey(1)

    # """show in multiple opencv-imshow windows"""
    # [cv2.namedWindow(window_name, flags=cv2.WINDOW_FREERATIO)
    #  for window_name in camera_ip_l]
    # while True:
    #     for window_name, q in zip(camera_ip_l, queue_list):
    #         cv2.imshow(window_name, q.get())
    #         cv2.waitKey(1)


def run_multi_camera_in_a_window():
    user_name, user_pwd = "admin", "admin123456"
    camera_ip_l = [
        "172.20.114.196",  # ipv4
        "[fe80::3aaf:29ff:fed3:d260]",  # ipv6
    ]

    mp.set_start_method(method='spawn')  # init
    queues = [mp.Queue(maxsize=4) for _ in camera_ip_l]

    processes = [mp.Process(target=image_collect, args=(queues, camera_ip_l))]
    for queue, camera_ip in zip(queues, camera_ip_l):
        processes.append(mp.Process(target=image_put, args=(queue, user_name, user_pwd, camera_ip)))

    for process in processes:
        process.daemon = True  # setattr(process, 'deamon', True)
        process.start()
    for process in processes:
        process.join()

def run():
    run_single_camera()  # quick, with 2 threads
    # run_multi_camera() # with 1 + n threads
    # run_multi_camera_in_a_window()  # with 1 + n threads
    pass

if __name__ == '__main__':
    run_single_camera()