import logging
import coloredlogs
import textwrap
coloredlogs.DEFAULT_FIELD_STYLES['levelname']['color'] = 'white'

print ("hello from logger")

class Logger(logging.Logger):
    def __init__(self,name):
        print ("setting up a new logger",name)
        super().__init__(name)
        self.setLevel(logging.INFO)
        format_str = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        formatter = coloredlogs.ColoredFormatter(format_str)

        ch = logging.StreamHandler()
        ch.setFormatter(formatter)
        self.addHandler(ch)

        file_formatter = logging.Formatter(format_str)
        fh = logging.FileHandler('coconnect.log',mode='a')
        fh.setFormatter(file_formatter)
        #fh.setLevel(logging.DEBUG)
        self.addHandler(fh)
