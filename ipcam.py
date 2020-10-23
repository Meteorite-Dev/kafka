import cv2
import time
import threading
from kafka import KafkaConsumer
import numpy as np

topic = "test-stream"

class ipcamCapture:
    def __init__(self, URL):
        self.Frame = []
        self.status = False
        self.isstop = False
	# 摄影机连接。
        self.capture = cv2.VideoCapture(URL)
        self.capture.set(cv2.CAP_PROP_FOURCC, cv2.VideoWriter_fourcc('M', 'J', 'P', 'G'))

    def start(self):
	# 把程序放进子线程，daemon=True 表示该线程会随着主线程关闭而关闭。
        print('ipcam started!')
        threading.Thread(target=self.queryframe, daemon=True, args=()).start()

    def stop(self):
	# 记得要设计停止无限循环的开关。
        self.isstop = True
        print('ipcam stopped!')
   
    def getframe(self):
	# 当有需要影像时，再回传最新的影像。
        return self.Frame
        
    def queryframe(self):
        while (not self.isstop):
            self.status, self.Frame = self.capture.read()
        
        self.capture.release()

URL = "rtsp://140.132.48.228"

# 连接摄影机
ipcam = ipcamCapture(0)

# 启动子线程
ipcam.start()

# 暂停1秒，确保影像已经填充
time.sleep(1)

# 使用无穷循环撷取影像，直到按下Esc键结束
while True:
    # 使用 getframe 取得最新的影像
    I = ipcam.getframe()
    
    cv2.imshow('Image', I)
    if cv2.waitKey(1) == 27:
        cv2.destroyAllWindows()
        ipcam.stop()
        break