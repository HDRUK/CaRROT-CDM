from .dag import *
from . import mapping_pipeline_helpers
from . import omop_db_inspect
#from omop_db_inspect import OMOPDetails
from . import extract

_DEBUG = False

def set_debug(value):
    global _DEBUG
    _DEBUG = value
