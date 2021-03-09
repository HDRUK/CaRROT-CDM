import os
import json
import pandas as pd
import numpy as np
from coconnect.cdm.operations import OperationTools
from coconnect.tools.logger import Logger


class ConvertDataType(Exception):
    pass

class FailedRequiredCheck(Exception):
    pass

class Base(object):
    def __init__(self,_type,_version='v5_3_1'):
        self.name = _type
        self.tools = OperationTools()
        self.logger = Logger(self.name)
        self.logger.info("Initialised Class")
        
        #load the cdm
        #get the field name, if it's required and the data type
        self.dir_path = os.path.dirname(os.path.realpath(__file__))
        self.cdm = pd.read_csv(f'{self.dir_path}/../../data/cdm/OMOP_CDM_{_version}.csv',encoding="ISO-8859-1")\
                     .set_index('table')\
                     .loc[_type].set_index('field')[['required', 'type']]
        
        #save the field names
        self.fields = self.cdm.index.values

        #create new attributes for all the fields in the CDM
        for field in self.fields:
            _type = self.cdm.loc[field]['type']
            _required = self.cdm.loc[field]['required']
            self.logger.debug(f'setting up the field {field} -- {_type} -- Required: {_required}')
            #initialise the field with value None
            setattr(self,field,None)
            
        #print a check to see what cdm objects have been initialised
        self.logger.debug(self.get_destination_fields())

    #default finalise does nothing
    def finalise(self,df):
        return df

    #default define does nothing
    def define(self,_):
        return self

    #get a list of all the destination fields loaded in this cdm
    def get_destination_fields(self):
        return list(self.fields)

    def execute(self,this):
        objs = {k:v for k,v in this.__dict__.items() if k!='logger' }
        self.__dict__.update(objs)
        self = self.define(self)

    def check_required(self,df):
        for col in df.columns:
            _required = self.cdm.loc[col]['required']
            self.logger.debug(f'checking if {col} is required')
            if _required == 'Yes':
                self.logger.debug(f'... it is required!')
                is_all_null = df[col].isnull().all()
                if is_all_null:
                    self.logger.error(f"{col} is a required field in this cdm, but it hasn't been filled or is corrupted")
                    raise FailedRequiredCheck(f"{col} has all NaN values, but it is a required field in the cdm") 
        
        
    def format(self,df):
        if not self.dtypes:
            return df

        for col in df.columns:
            _type = self.cdm.loc[col]['type']
            self.logger.debug(f'applying formatting to {_type} for field {col}')

            try:
                df[col] = self.dtypes[_type](df[col])
            except:
                self.logger.error(df[col])
                self.logger.error(f'failed to convert {col} to {_type}')      
                return None
                #raise ConvertDataType(f'failed to convert {col} to {_type}')
            
        return df
        
    def get_df(self):
        dfs = {
            key: getattr(self,key).rename(key)
            for key in self.fields
            if getattr(self,key) is not None
        }
        
        if len(dfs) == 0:
            return None

        df = pd.concat(dfs.values(),axis=1)

        missing_fields = set(self.fields) - set(df.columns)

        for field in missing_fields:
            df[field] = np.NaN

        df = df[self.fields]

        return df
