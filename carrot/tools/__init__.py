import carrot
import os
import sys
import json
import glob
import inspect
import time

from . import extract
from .dag import make_dag,make_report_dag

from . import bclink_helpers 

from .file_helpers import (
    load_json,
    load_json_delta,
    load_csv,
    load_tsv,
    load_sql,
    create_csv_store,
    create_sql_store,
    create_bclink_store,
    remove_missing_sources_from_rules,
    filter_rules_by_destination_tables,
    filter_rules_by_object_names,
    get_separator_from_filename,
    get_file_map_from_dir,
    get_mapped_fields_from_rules,
    get_source_tables_from_rules,
    get_subfolders,
    get_files,
    diff_csv
)

from .rules_helpers import(
    get_person_ids,
    get_source_field,
    get_source_table,
    apply_rules,
    load_from_file
)

_DEBUG = False

def set_debug(value):
    global _DEBUG
    _DEBUG = value
    
def get_classes(format=False):
    retval = get_classes_from_tool(format=format)
    config_folder = os.environ.get('CARROT_CONFIG_FOLDER')
    if config_folder is not None:
        sys.path.append(config_folder)
        files = [x for x in os.listdir(config_folder) if x.endswith(".py") and not x.startswith('__')]
        for fname in files:
            mname = fname.split(".")[0]
            module = __import__(mname,fromlist=[fname])
            path = os.path.join(config_folder,fname)
            defined_classes = {
                fname: {
                    'module':m[1].__module__,
                    'path': path,
                    'last-modified': time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(os.path.getmtime(os.path.join(config_folder,fname))))
                }
                for m in inspect.getmembers(module, inspect.isclass)
                if m[1].__module__ == module.__name__
            }
            retval.update(defined_classes)

    return retval
    
def get_classes_from_tool(format=format):
    from carrot.cdm import classes
    _dir = os.path.dirname(classes.__file__)
    files = [x for x in os.listdir(_dir) if x.endswith(".py") and not x.startswith('__')]
    retval = {}
    for fname in files:
        path = os.path.join(_dir,fname)
        mname = fname.split(".")[0]
        mname = '.'.join([classes.__name__, mname])
        if os.path.islink(path):
            link = os.readlink(path)
            if os.path.isfile(link) == False:
                os.unlink(path)
                continue
        
        module = __import__(mname,fromlist=[fname])
        defined_classes = {
            fname: {
                'module':m[1].__module__,
                'path': path  if not os.path.islink(path) else os.readlink(path),
                'sympath': path,
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

