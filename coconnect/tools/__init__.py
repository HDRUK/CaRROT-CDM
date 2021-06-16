import coconnect
import pandas as pd
import os
import json
import glob
import inspect

from . import extract
from .dag import make_dag

from .file_helpers import (
    load_json,
    load_csv
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

def get_source_field(table,name):
    if name not in table:
        if name.lower() in table:
            return table[name.lower()]
        else:
            raise FieldNotFoundError(f"Cannot find {name} in table {table.name}. Options are {table.columns.tolist()}")
    return table[name]

def get_source_table(inputs,name):
    #make a copy of the input data column slice
    if name not in inputs.keys():
        short_keys = {key[:31]:key for key in inputs.keys()}
        if name in short_keys:
            name = short_keys[name]
        elif name.lower() in short_keys:
            name = short_keys[name.lower()]
        else:
            raise TableNotFoundError(f"Cannot find {name} in inputs. Options are {inputs.keys()}")
    inputs[name].name = name
    return inputs[name]

def apply_rules(cdm,obj,rules):
    for destination_field,rule in rules.items():
        source_table_name = rule['source_table']
        source_field_name = rule['source_field']
        operations = None
        if 'operations' in rule:
            operations = rule['operations']
        term_mapping = None
        if 'term_mapping' in rule:
            term_mapping = rule['term_mapping']


        source_table = get_source_table(cdm.inputs,source_table_name)
        source_field = get_source_field(source_table,source_field_name)
        series = source_field.copy()

        if operations is not None:
            for operation in operations:
                function = cdm.tools[operation]
                series = function(series)
                
        if term_mapping is not None:
            if isinstance(term_mapping,dict):
                # value level mapping
                # - term_mapping is a dictionary between values and concepts
                # - map values in the input data, based on this map
                series = series.map(term_mapping)
            else:
                # field level mapping.
                # - term_mapping is the concept_id
                # - set all values in this column to it
                series.values[:] = term_mapping

        obj[destination_field].series = series
