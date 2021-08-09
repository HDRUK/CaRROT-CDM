#import coconnect.tools as tools
import logging
import coloredlogs
import textwrap
coloredlogs.DEFAULT_FIELD_STYLES['levelname']['color'] = 'white'


class CustomFormatter(coloredlogs.ColoredFormatter):
    def __init__(self,format_str):
        super().__init__(format_str)
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
        format_str = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        formatter = CustomFormatter(format_str)

        ch = logging.StreamHandler()
        ch.setFormatter(formatter)
        self.addHandler(ch)

        file_formatter = logging.Formatter(format_str)
        fh = logging.FileHandler('coconnect.log',mode='a')
        fh.setFormatter(file_formatter)
        #fh.setLevel(logging.DEBUG)
        self.addHandler(fh)
