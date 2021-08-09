import coconnect
import logging
import coloredlogs
import textwrap
coloredlogs.DEFAULT_FIELD_STYLES['levelname']['color'] = 'white'

class Logger(logging.Logger):
    def __init__(self,name):
        super().__init__(name)
        debug_level = coconnect.params['debug_level']

        if debug_level == 0:
            debug_level = logging.ERROR
        elif debug_level == 1:
            debug_level = logging.WARNING
        elif debug_level == 2:
            debug_level = logging.INFO
        else:
            debug_level = logging.DEBUG
            
        self.setLevel(debug_level)
        format_str = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        formatter = coloredlogs.ColoredFormatter(format_str)

        ch = logging.StreamHandler()
        ch.setFormatter(formatter)
        self.addHandler(ch)

        file_formatter = logging.Formatter(format_str)
        fh = logging.FileHandler('coconnect.log',mode='a')
        fh.setFormatter(file_formatter)
        fh.setLevel(debug_level)
        self.addHandler(fh)
