
class ReadCounter(object):
    def __init__(self):
        self.value = 0
        self._lock = threading.Lock()
        
    def increment(self):
        with self._lock:
            self.value += 1


class thread(threading.Thread):
    def __init__(self, thread_name, thread_ID):
        threading.Thread.__init__(self)
        self.thread_name = thread_name
        self.thread_ID = thread_ID

        # helper function to execute the threads
    def run(self):
        print(str(self.thread_name) +" "+ str(self.thread_ID));            

"""
now = datetime.now()
time = now.strftime("%H:%M:%S")
counter = 0
prevTime = time

def OrderCounter():
    now = datetime.now()
    time = now.strftime("%H:%M:%S")
    if(time!=prevTime):
        print(prevTime,"-",counter)
        prevTime = time
    counter +=1
"""