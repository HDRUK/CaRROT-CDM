import os
import json
import pandas as pd
import numpy as np
import collections
from coconnect.cdm.operations import OperationTools
from coconnect.tools.logger import Logger

class RequiredFieldIsNone(Exception):
    pass

class ConvertDataType(Exception):
    pass

class FailedRequiredCheck(Exception):
    pass

class BadInputs(Exception):
    pass

class DataType(object):
    def __init__(self, dtype: str, required: bool, pk=False):
        self.series = None
        self.dtype = dtype
        self.required = required
        self.pk = pk

class DataFormatter(collections.OrderedDict):
    def __init__(self):
        super().__init__()
        self['INTEGER'] = lambda x : pd.to_numeric(x,errors='coerce').astype('Int64')
        self['FLOAT'] = lambda x : pd.to_numeric(x,errors='coerce').astype('Float64')
        self['VARCHAR(60)'] = lambda x : x.fillna('').astype(str).apply(lambda x: x[:60])
        self['VARCHAR(50)'] = lambda x : x.fillna('').astype(str).apply(lambda x: x[:50])
        self['VARCHAR(20)'] = lambda x : x.fillna('').astype(str).apply(lambda x: x[:20])
        self['VARCHAR(10)'] = lambda x : x.fillna('').astype(str).apply(lambda x: x[:10])
        self['VARCHAR'] = lambda x : x.fillna('').astype(str).apply(lambda x: x)
        self['STRING(50)'] = lambda x : x.fillna('').astype(str).apply(lambda x: x[:50])
        self['DATETIME'] = lambda x : pd.to_datetime(x,errors='coerce').dt.strftime('%Y-%m-%d %H:%M:%S')
        self['DATE'] = lambda x : pd.to_datetime(x,errors='coerce').dt.date

        
class Base(object):
    """
    Common object that all CDM objects inherit from
    """
    def __init__(self,_type,_version='v5_3_1'):
        """
        Initialise the CDM Base Object class
        Args:
           _type (str): the name of the object being initialsed, e.g. "person"
           _version (str): the CDM version, see https://github.com/OHDSI/CommonDataModel/tags
        Returns: 
           None
        """
        self.name = _type
        self._type = _type
        self._meta = {}
        self.logger = Logger(self.name)

        self.dtypes = DataFormatter()
        self.fields = self.get_field_names()

        if len(self.fields) == 0:
            raise Exception("something misconfigured - cannot find any DataTypes for {self.name}")

        #print a check to see what cdm objects have been initialised
        self.logger.debug(self.get_destination_fields())
        self.__df = None

    def get_field_names(self):
        return [
            item
            for item in self.__dict__.keys()
            if isinstance(getattr(self,item),DataType)
        ]

    def get_ordering(self):
        return [
            field
            for field in self.fields
            if getattr(self,field).pk == True
        ]
        
    def __getitem__(self, key):
        return getattr(self, key)

    def __setitem__(self, key, obj):
        return setattr(self, key, obj)
    
    def set_name(self,name):
        self.name = name
        self.logger.name = self.name
    
    def define(self,_):
        """
        define function, expected to be overloaded by the user defining the object
        """
        return self

    def get_destination_fields(self):
        """
        Get a list of all the destination fields that have been 
        loaded and associated to this cdm object

        
        Returns: 
           list: a list of all the destination fields that have been defined
        """
        return list(self.fields)

    def execute(self,that):
        """
        execute the creation of the cdm object by passing

        Args:
           that: input object class where input objects can be loaded 
                 and the define/finalise functions can be overloaded
        """

        #extract all objects from the passed object
        objs = {k:v for k,v in that.__dict__.items() if k!='logger' }
        #add objects to this class
        self.__dict__.update(objs)

        #execute the define function that is likely to define the cdm fields based on inputs
        self = self.define(self)

        #build the dataframe for this object
        _ = self.get_df()
        
    def get_df(self,force_rebuild=False):
        """
        Retrieve a dataframe from the current object

        Returns:
           pandas.Dataframe: extracted dataframe of the cdm object
        """
        #if the dataframe has already been built.. just return it
        if not self.__df is None and not force_rebuild:
            return self.__df 
        
        #get a dict of all series
        #each object is a pandas series
        dfs = {}

        for field in self.fields:
            obj = getattr(self,field)
            series = obj.series
            if series is None:
                #if required:
                #    self.logger.error(f"{field} is all null/none or has not been set/defined")
                #    raise RequiredFieldIsNone(f"{field} is a required for {self.name}.")
                continue

            #rename the column to be the final destination field name
            series = series.rename(field)
            #register the new series
            dfs[field] = series

        #if there's none defined, dont do anything
        if len(dfs) == 0:
            return None

        #check the lengths of the dataframes
        lengths = list(set([len(df) for df in dfs.values()]))
        if len(lengths)>1:
            self.logger.error("One or more inputs being mapped to this object has a different number of entries")
            for name,df in dfs.items():
                self.logger.error(f"{name} of length {len(df)}")
            raise BadInputs("Differring number of rows in the inputs")

        #create a dataframe from all the series objects
        df = pd.concat(dfs.values(),axis=1)

        #find which fields in the cdm havent been defined
        missing_fields = set(self.fields) - set(df.columns)

        self._meta['defined_columns'] = df.columns.tolist()
        self._meta['undefined_columns'] = list(missing_fields)
                
        #set these to a nan/null series
        for field in missing_fields:
            df[field] = np.NaN

        #simply order the columns 
        df = df[self.fields]

        df = self.finalise(df)
        df = self.format(df)

        #register the df
        self.__df = df
        return df

    def format(self,df):
        for col in df.columns:
            obj = getattr(self,col)
            dtype = obj.dtype
            formatter_function = self.dtypes[dtype]
            df[col] = formatter_function(df[col])
        
        return df

    def finalise(self,df):
        """
        Finalise function, expected to be overloaded by children classes
        """
        
        required_fields = [
            field
            for field in self.get_field_names()
            if getattr(self,field).required == True
        ]
        self._meta['required_fields'] = {}
        for field in required_fields:
            nbefore = len(df)
            df = df[~df[field].isna()]
            nafter = len(df)

            ndiff = nbefore - nafter
            if ndiff>0:
                self.logger.warning(f"Requiring non-null values in {field} removed {ndiff} rows, leaving {nafter} rows.")
            self._meta['required_fields'][field] = {
                'before':nbefore,
                'after':nafter
            }
            #if 'concept_id' in field:
            #    values = df[field].unique().tolist()
            #    self._meta['required_fields'][field]['concept_values'] = values


        df = df.sort_values(self.get_ordering())
        
        return df

