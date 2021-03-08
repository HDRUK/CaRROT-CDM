import os
import json
import pandas as pd
import numpy as np
from coconnect.cdm.operations import OperationTools


class Base(object):
    def __init__(self,_type):
        self.name = _type
        self.tools = OperationTools()
        #load the cdm
        #get the field name, if it's required and the data type
        self.dir_path = os.path.dirname(os.path.realpath(__file__))
        self.cdm = pd.read_csv(f'{self.dir_path}/../../data/cdm/OMOP_CDM_v5_3_1.csv',encoding="ISO-8859-1")\
                     .set_index('table')\
                     .loc[_type].set_index('field')[['required', 'type']]
        
        #save the field names
        self.fields = self.cdm.index.values

        #create new attributes for all the fields in the CDM
        for field in self.fields:
            setattr(self,field,None)

        #print a check to see what cdm objects have been initialised
        print (self.get_destination_fields())

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
        self.__dict__.update(this.__dict__)
        self = self.define(self)

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
