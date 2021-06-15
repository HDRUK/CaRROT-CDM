#import coconnect.tools as tools
import logging
import coloredlogs
import textwrap
coloredlogs.DEFAULT_FIELD_STYLES['levelname']['color'] = 'white'


class CustomFormatter(coloredlogs.ColoredFormatter):
    def __init__(self):
        super().__init__('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    # def format(self, record):
    #     msg = record.msg
    #     record.msg = ''
    #     header = super().format(record)

    #     if not isinstance(msg,str):
    #         msg = msg.__str__()
    #     msg = textwrap.indent(msg, ' ' * len(header)).strip()
    #     return header + msg


class Logger(logging.Logger):
    def __init__(self,name):
        super().__init__(name)
        self.setLevel(logging.INFO)
        #if tools._DEBUG:
        #    self.setLevel(logging.DEBUG)
        ch = logging.StreamHandler()
        formatter = CustomFormatter()
        ch.setFormatter(formatter)
        self.addHandler(ch)

