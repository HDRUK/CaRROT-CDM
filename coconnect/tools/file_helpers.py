import os
import json
import pandas as pd

class MissingInputFiles(Exception):
    pass

class InputData:
    def __init__(self,chunksize):
        self.chunksize = chunksize
        self.__file_readers = {}
        self.__dataframe = {}

    def all(self):
        return {
            key:self[key]
            for key in self.keys()
        }
        
    def keys(self):
        return self.__file_readers.keys()

    def next(self):
        check = {}
        for key in self.keys():
            try:
                self.get_df(key)
            except StopIteration:
                self.__dataframe[key] = pd.DataFrame(columns = self.__dataframe[key].columns)
                
            check[key] = self.__dataframe[key].empty

        if all([x==True for x in check.values()]):
            raise StopIteration("all input files have been used now")
                        
    def get_df(self,key):
        self.__dataframe[key] = self.__file_readers[key].get_chunk(self.chunksize)

    def __getitem__(self,key):
        if key not in self.__dataframe.keys():
            self.get_df(key)
        return self.__dataframe[key]
        
    def __setitem__(self,key,obj):
        self.__file_readers[key] = obj
        
    
    

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


def load_csv(_map,chunksize=None,nrows=None,lower_col_names=False,load_path="",rules=None):

    if rules is not None:
        rules = load_json(rules)
        source_map = get_mapped_fields_from_rules(rules)

        inputs_from_json = list(source_map.keys())
        inputs_from_cli = list(_map.keys())

        missing_inputs = list(set(inputs_from_json) - set(inputs_from_cli))
        if len(missing_inputs) > 0 :
            raise MissingInputFiles (f"Found the following files {missing_inputs} in the json file, that are not in the loaded file list... {inputs_from_cli}")
        
        #reduce the mapping of inputs, if we dont need them all
        _map = {
            k: {
                'file':v,
                'fields':source_map[k]
            }
            for k,v in _map.items()
            if k in source_map
        }

        print (_map.keys())
        exit(0)
        

    if chunksize == None:
        retval = {}
    else:
        retval = InputData(chunksize)
        
    for key,obj in _map.items():
        fields = None
        if isinstance(obj,str):
            fname = obj
        else:
            fname = obj['file']
            fields = obj['fields']

        df = pd.read_csv(load_path+fname,chunksize=chunksize,nrows=nrows,dtype=str,usecols=fields)

        if isinstance(df,pd.DataFrame):
            #this should be removed
            if lower_col_names:
                df.columns = df.columns.str.lower()

        retval[key] = df

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
    
