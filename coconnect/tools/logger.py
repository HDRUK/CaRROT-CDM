import coconnect.tools as tools
import logging
import coloredlogs
coloredlogs.DEFAULT_FIELD_STYLES['levelname']['color'] = 'white'



class Logger(logging.Logger):
    def __init__(self,name):
        super().__init__(name)
        self.setLevel(logging.INFO)
        #if tools._DEBUG:
        #    self.setLevel(logging.DEBUG)
        ch = logging.StreamHandler()
        formatter = coloredlogs.ColoredFormatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        ch.setFormatter(formatter)
        self.addHandler(ch)

