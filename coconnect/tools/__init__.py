from .dag import *
from . import mapping_pipeline_helpers
from . import extract

_DEBUG = False

def set_debug(value):
    global _DEBUG
    _DEBUG = value
