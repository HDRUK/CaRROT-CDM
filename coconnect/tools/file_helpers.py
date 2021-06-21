import os
import json
import pandas as pd

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


def load_csv(_map,nrows=None,skiprows=None,lower_col_names=False,load_path="",rules=None):

    if rules is not None:
        rules = load_json(rules)
        source_map = get_mapped_fields_from_rules(rules)
        
        #reduce the mapping of inputs, if we dont need them all
        _map = {
            k: {
                'file':v,
                'fields':source_map[k]
            }
            for k,v in _map.items()
            if k in source_map
        }
        

    
    for key,obj in _map.items():
        fields = None
        if isinstance(obj,str):
            fname = obj
        else:
            fname = obj['file']
            fields = obj['fields']

        df = pd.read_csv(load_path+fname,nrows=nrows,skiprows=skiprows,dtype=str)
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
 

def get_mapped_fields_from_rules(rules):
    #extract a tuple of source tables and source fields
    sources = [
        (x['source_table'],x['source_field'])
        for cdm_obj_set in rules['cdm'].values()
        for cdm_obj in cdm_obj_set
        for x in cdm_obj.values()
    ]
    
    source_map = {}
    for (table,field) in sources:
        if table not in source_map:
            source_map[table] = []
        source_map[table].append(field)
            
    source_map = {
        k:list(set(v))
        for k,v in source_map.items()
    }

    return source_map
    
