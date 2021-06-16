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
 
