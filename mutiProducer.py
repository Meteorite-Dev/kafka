from kafka import KafkaProducer
import cv2
import time
from multiprocessing import Process , Pool
from functools import partial



def cap_video(tp):
    producer = KafkaProducer(bootstrap_servers=['node04:9092'],max_request_size=32000000)
    capture = cv2.VideoCapture(str(tp[0]))
    capture.set(cv2.CAP_PROP_FOURCC, cv2.VideoWriter_fourcc('M', 'J', 'P', 'G'))
    print("Process ",tp[1] ," start!")

    while True :
        status, Frame = capture.read()
        Frame = cv2.resize(Frame ,(640,480))
        buffer = cv2.imencode('.jpg', Frame)[1]
        
        res , buffer = cv2.imencode('.jpg', Frame)
        st = int(time.time()*1000) % 86400000
        cv2.imshow("Image"+str(tp[1]), Frame)
        mes = str(st)
        # print("process " , tp[1] ," : " ,mes)
        # print(tp[2])
        res = producer.send(str(tp[2]) ,value=buffer.tobytes() , key=mes.encode('utf-8'))
        # print(res)
        if cv2.waitKey(1) == 27:
            capture.release()
            cv2.destroyAllWindows()
            print("stop")
            break
        # time.sleep(0.02)

if __name__ =="__main__":
    path=["/mnt/sdd1/video/2020-10-20_12:00:01.avi",
          "/mnt/sdd1/video/2020-10-16_12:00:01.avi",
          "/mnt/sdd1/video/2020-10-16_11:00:01.avi",
          "/mnt/sdd1/video/2020-10-19_15:50:01.avi",
          "/mnt/sdd1/video/2020-10-20_15:50:01.avi"]
    num = list(range(5))
    # print(num)
    topics = ['thread0','thread1','thread2','thread3','thread4']
    tp = []
    for n in num:
        tp.append((path[n] , n ,topics[n]))
        print(tp[n])
    
    pool = Pool()
    # cap = partial(cap_video , num=range(5))

    res = pool.map(cap_video , tp)
    pool.close()
    pool.join()
    print(res)
