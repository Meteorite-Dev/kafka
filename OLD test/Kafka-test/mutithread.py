import threading

def th_job(num):
    for i in range(5):
        print("thread {0} . {1}".format(num , i))

def main():
    t_list = []
    t1 = threading.Thread(target=th_job , args=(1,))
    t_list.append(t1)
    t2 = threading.Thread(target=th_job , args=(2,))
    t_list.append(t2)
    t3 = threading.Thread(target=th_job , args=(3,))
    t_list.append(t3)
    
    for t in t_list:
        t.start()

if __name__ =='__main__':
    main()
