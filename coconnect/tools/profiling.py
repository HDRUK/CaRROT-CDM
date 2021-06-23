import os
import time
import threading
import psutil
import pandas as pd
from coconnect.tools.logger import Logger
        

class Profiler:
    def __init__(self,name=None,interval=0.1):

        if name == None:
            name = self.__class__.__name__
        else:
            name = f"{self.__class__.__name__}_{name}"
            
        self.logger = Logger(self.__class__.__name__)
        
        self.pid = os.getpid()
        self.py = psutil.Process(self.pid)

        self.interval = interval
        self.logger.info(f"tracking {self.pid} every {self.interval} seconds")
        self.cpu_count = psutil.cpu_count()
        self.logger.info(f"{self.cpu_count} cpus being used")

        self.th = threading.Thread(target=self.track)

        self.tracking = []
        self.init_time = time.time()
        self._stop = False
        self._df = None
        
    def start(self):
        self.logger.info("starting profiling")
        self.th.start()

    def stop(self):
        self._stop = True
        self.th.join()
        self.logger.info("finished profiling")

    def get_df(self):
        if self._df is None:
            self._df = pd.DataFrame(self.tracking)
        return self._df
        
    def summary(self):
        self.logger.info(self.get_df())
        
    def track(self):
        while self._stop == False:
            memory = self.py.memory_info()[0]/2.**30
            cpu = self.py.cpu_percent() / self.cpu_count
            current_time = time.time() - self.init_time
            info = {'time[s]':current_time,'memory[GB]':memory,'cpu[%]':cpu}
            self.tracking.append(info)
            time.sleep(self.interval)

        self.summary()
