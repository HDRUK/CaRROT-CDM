import coconnect
import os
import json
import glob
import inspect

from . import extract
from .dag import make_dag

from .file_helpers import (
    load_json,
    load_csv,
    get_file_map_from_dir,
    get_mapped_fields_from_rules
)

from .rules_helpers import(
    get_source_field,
    get_source_table,
    apply_rules
)

_DEBUG = False

def set_debug(value):
    global _DEBUG
    _DEBUG = value
    
def get_classes(format=False):
    import time
    from coconnect.cdm import classes
    _dir = os.path.dirname(classes.__file__)
    files = [x for x in os.listdir(_dir) if x.endswith(".py") and not x.startswith('__')]
    retval = {}
    for fname in files:
        mname = fname.split(".")[0]
        mname = '.'.join([classes.__name__, mname])
        module = __import__(mname,fromlist=[fname])
        path = os.path.join(_dir,fname)
        defined_classes = {
            m[0]: {
                'module':m[1].__module__,
                'path': path  if not os.path.islink(path) else os.readlink(path),
                'last-modified': time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(os.path.getmtime(os.path.join(_dir,fname))))
            }
            for m in inspect.getmembers(module, inspect.isclass)
            if m[1].__module__ == module.__name__
        }
        retval.update(defined_classes)
    if format:
        return json.dumps(retval,indent=6)
    else:
        return retval

