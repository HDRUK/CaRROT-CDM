from coconnect.tools.logger import Logger

class DataCollection:
    def __init__(self):
        self.logger = Logger(self.__class__.__name__)
        self.logger.info("DataCollection Object Created")
