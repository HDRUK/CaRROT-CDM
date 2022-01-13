from ._version import __version__
params = {
    'debug_level':2,
    'log_file': 'coconnect.log',
    'version':__version__,
    'cdm':'5.3.1'
}
from . import cdm

