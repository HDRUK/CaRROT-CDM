from types import SimpleNamespace
import pandas as pd
import os
import json
import glob
import inspect
from .dag import *
from . import mapping_pipeline_helpers
from . import omop_db_inspect
#from omop_db_inspect import OMOPDetails
from . import extract

_DEBUG = False

def set_debug(value):
    global _DEBUG
    _DEBUG = value

def load_json(f_in):
    try:
        data = json.load(open(f_in))
    except FileNotFoundError as err:
        try:
            data_dir = os.path.abspath(
                os.path.join(
                    os.path.dirname(__file__),'..','data')
                )
            data =  json.load(open(f'{data_dir}/{f_in}'))
        except FileNotFoundError:
            raise FileNotFoundError(err)

    return data



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





def load_csv(_map,nrows=None,lower_col_names=True,load_path=""):

    for key,obj in _map.items():
        fields = None
        if isinstance(obj,str):
            fname = obj
        else:
            fname = obj['file']
            fields = obj['fields']

        df = pd.read_csv(load_path+fname,nrows=nrows,dtype=str)
        for col in df.columns:
            df[col].fname = fname

        if lower_col_names:
            df.columns = df.columns.str.lower()

        #filter on only the fields we need
        if fields is not None:
            df = df[fields]
        
        _map[key] = df 
    return _map

def get_file_map_from_dir(_dir):
    if not os.path.isdir(_dir):
        _dir = os.path.abspath(
            os.path.join(
                os.path.dirname(__file__),'..','data',_dir)
        )

    _map = {}
    for fname in glob.glob(f"{_dir}/*.csv"):
        key = fname.split("/")[-1]
        _map[key] = fname
    
    return _map

def to_name_space(_map):
    return SimpleNamespace(**_map)

