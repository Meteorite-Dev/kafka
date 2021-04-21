from kafka import KafkaProducer
import cv2
import time
from multiprocessing import Process , Pool
producer = KafkaProducer(bootstrap_servers=['node04:9092'],max_request_size=32000000)

def cap_video(tp):
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
    path=["/mnt/sdd1/video/2020-10-20_12:00:01.avi"]
    num = list(range(1))
    # print(num)
    topics = ['thread1']
    tp = []
    for n in num:
        tp.append((path[n] , n ,topics[n]))
        print(tp[n])
    
    cap_video(tp[0])