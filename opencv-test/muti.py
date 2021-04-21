import cv2 
from multiprocessing import Process, cpu_count



def capture ():
    cap = cv2.VideoCapture(0)

    while True :
        ret , frame = cap.read()
        cv2.imshow("image" , frame)
        if cv2.waitKey(1)& 0xFF ==ord('q'):
            cap.release()
            cv2.destroyAllWindows()
            break
        
def cap_video(path):
    cap = cv2.VideoCapture(path)

    while True :
        ret , frame = cap.read()
        cv2.imshow("image" , frame)
        if cv2.waitKey(1)& 0xFF ==ord('q'):
            cap.release()
            cv2.destroyAllWindows()
            break

if __name__ =='__main__':
    path="/mnt/sdd1/video/2020-10-15_21:35.avi"
    cap_video(path)








