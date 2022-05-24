import os
import time
import threading
import psutil
import pandas as pd
from carrot.tools.logger import Logger
        

class Profiler(Logger):
    def __init__(self,name=None,interval=0.1):
        
        if name == None:
            name = self.__class__.__name__
        else:
            name = f"{self.__class__.__name__}_{name}"
            

        #retrieve the process id for the current run
        self.pid = os.getpid()
        #create a psutil instance to montior this
        self.py = psutil.Process(self.pid)
        #set the interval (seconds) of how often to check the cpu and memory
        self.interval = interval
        self.logger.info(f"tracking {self.pid} every {self.interval} seconds")
        #count the number of cpus the computer running this process has
        self.cpu_count = psutil.cpu_count()
        self.logger.info(f"{self.cpu_count} cpus available")
        #initiate a threaded function
        #that will run in a separate process and can monitor CPU/memory in the background
        self.th = threading.Thread(target=self.track)

        #init some global variables
        self.tracking = []
        self.init_time = time.time()
        self._stop = False
        self._df = None
        
    def start(self):
        #start the thread
        self.logger.info("starting profiling")
        self.th.start()

    def stop(self):
        #stop the thread
        self._stop = True
        self.th.join()
        self.logger.info("finished profiling")

    def get_df(self):
        #build a little dataframe for cpu/memory v.s. time,
        #if it has not been built already
        if self._df is None:
            self._df = pd.DataFrame(self.tracking)
        return self._df
        
    def summary(self):
        #print the dataframe created for cpu/memory v.s. time
        self.logger.info(self.get_df())
        
    def track(self):
        """
        Main function to profile CPU and memory usage
        """
        #while the program has been told to profile the usage
        while self._stop == False:
            #from the current process, calculate the current memory usage (in GB)
            memory = self.py.memory_info()[0]/2.**30
            #also calculate the CPU % in use at this epoch in time
            cpu = self.py.cpu_percent() / self.cpu_count
            #calcuate the current time - time since the start of the process
            current_time = time.time() - self.init_time
            #log the data
            info = {'time[s]':current_time,'memory[GB]':memory,'cpu[%]':cpu}
            self.tracking.append(info)
            #sleep the number of seconds requested
            time.sleep(self.interval)

        #once finished, call the summary function
        self.summary()
