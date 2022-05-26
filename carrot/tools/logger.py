import carrot
import logging
DEBUG_LEVELV_NUM = 9 
logging.addLevelName(DEBUG_LEVELV_NUM, "NOTICE")
def notice(self, message, *args, **kws):
    self._log(DEBUG_LEVELV_NUM, message, args, **kws) 
logging.Logger.notice = notice


logging.addLevelName(10, "TEXT")
def spam(self, message, *args, **kws):
    self._log(10, message, args, **kws) 
logging.Logger.info_v2 = spam

import coloredlogs
import textwrap
import os
coloredlogs.DEFAULT_FIELD_STYLES['levelname']['color'] = 'white'

coloredlogs.DEFAULT_LEVEL_STYLES['text'] = {'color': 219}


class _Logger(logging.Logger):
    def __init__(self,name):
        super().__init__(name)
        
        save_to_file = carrot.params['log_file']
        debug_level = carrot.params['debug_level']
        if debug_level < 0:
            debug_level = logging.CRITICAL
        elif debug_level == 0:
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

        if save_to_file:
            self.debug(f"Saving logs to file {save_to_file}")
            file_formatter = logging.Formatter(format_str)
            _dir = os.path.realpath(os.path.dirname(save_to_file))
        
            if not os.path.exists(_dir):
                os.makedirs(_dir)
        
                fh = logging.FileHandler(save_to_file,mode='a')
                fh.setFormatter(file_formatter)
                fh.setLevel(debug_level)
                self.addHandler(fh)
        
        
class Logger():
    @property
    def logger(self):
        return _Logger(type(self).__name__)
