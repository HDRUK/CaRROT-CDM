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

class BadInputs(Exception):
    pass

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
        self.tools = OperationTools()
        self.logger = Logger(self.name)
        self.logger.debug("Initialised Class")
        
        #load the dir path of where this file is
        dir_path = os.path.dirname(os.path.realpath(__file__))
        #load the details of this cdm objects from the data files taken from OHDSI GitHub
        # - set the table (e.g. person, condition_occurrence,...)  as the index
        #   so that all values associated with the object (name) can be retrieved
        # - then set the field (e.g. person_id, birth_datetime,,) to help with future lookups
        # - just keep information on if the field is required (Yes/No) and what the datatype is (INTEGER,..)
        self.cdm = pd.read_csv(f'{dir_path}/../../data/cdm/OMOP_CDM_{_version}.csv',encoding="ISO-8859-1")\
                     .set_index('table')\
                     .loc[self.name].set_index('field')[['required', 'type']]

        self.cdm['is_source'] = self.cdm.index.str.endswith("_source_value")
        
        #extract all the fields (destination_fields) associated with this cdm object
        self.fields = self.cdm.index.values
        
        #create new attributes for all the fields in the CDM
        for field in self.fields:
            #extract the datatype
            _type = self.cdm.loc[field]['type']
            #extract if it is required to be filled or not
            _required = self.cdm.loc[field]['required']
            self.logger.debug(f'setting up the field {field} -- {_type} -- Required: {_required}')
            #initialise the field with value None
            setattr(self,field,None)
            
        #print a check to see what cdm objects have been initialised
        self.logger.debug(self.get_destination_fields())

    def finalise(self,df):
        """
        Finalise function, expected to be overloaded by children classes
        """
        if 'person_id' in df.columns:

            masker = {
                x:i+1 #start from 1
                for i,x in enumerate(sorted(df['person_id'].unique()))
            }
            
            df['person_id'] = df['person_id'].map(masker)
            self.logger.info(f"Just masked person_id")# \n {df['person_id']}")

        return df


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


    def check_required(self,df):
        """
        Check if the columns in the input dataframe are required or not
        If one is a required field, and all the values are NaN, raised an Exception

        Args:
           df (pandas.Dataframe): pandas dataframe to check

        """

        for col in df.columns:
            _required = self.cdm.loc[col]['required']
            self.logger.debug(f'checking if {col} is required')
            if _required == 'Yes':
                self.logger.debug(f'... it is required!')
                is_all_null = df[col].isnull().all()
                if is_all_null:
                    self.logger.error(f"{col} is a required field in this cdm, but it hasn't been filled or is corrupted")
                    raise FailedRequiredCheck(f"{col} has all NaN values, but it is a required field in the cdm") 
        
        
    def format(self,df,raise_error=True):
        """
        Format the dataframe into the right output format i.e. int/str/datetime...

        Args:
           df (pandas.Dataframe): input dataframe
           raise_error (bool): decide whether to raise an error or just null df
        Return:
           pandas.Dataframe: formatted pandas dataframe

        """
        #if the datatypes for this object haven't be defined, then cant do any formatting
        if not self.dtypes:
            return df

        #loop over all columns (series) in the dataframe
        for col in df.columns:
            #extract the datatype associated to this colun
            _type = self.cdm.loc[col]['type']
            self.logger.debug(f'applying formatting to {_type} for field {col}')

            #pull the function of how to convert the column and convert it
            try:
                convert_function = self.dtypes[_type]
                df[col] = convert_function(df[col])
            except KeyError:
                raise 
            except Exception as err:
                self.logger.error(err)

                print (pd.to_numeric(df[col],errors='coerce').astype('Int64'))
                
                self.logger.error(df[col])
                self.logger.error(f'failed to convert {col} to {_type}')
                self.logger.error(f'this is likely coming from the definition {self.define.__name__}')
                self.logger.error('this has the following unique values...')
                self.logger.error(df[col].unique())

                
                if raise_error:
                    raise ConvertDataType(f'failed to convert {col} to {_type}')
                else:
                    df[col] = np.NaN
            
        return df
        
    def get_df(self):
        """
        Retrieve a dataframe from the current object

        Returns:
           pandas.Dataframe: extracted dataframe of the cdm object
        """

        #get a dict of all series
        #each object is a pandas series
        dfs = {}
        for key in self.fields:
            series = getattr(self,key)
                        
            if series is None:
                continue
            series = series.rename(key)
            series = series.sort_index()
            dfs[key] = series

        # non_series = [k for k,v in dfs.items() if isinstance(v,str) ]
        # if len(non_series) == len(dfs.keys()):
        #     self.logger.error("All series are strings! They should be pandas series or dataframes!")
        #     raise BadInputs("Can't find any pandas dataframes")

        # elif len(non_series)>0:
        #     good_series = list(set(dfs.keys()) - set(non_series))[0]
        #     for key in non_series:
        #         self.logger.warning(f'attempting to set the field "{key}" to a string'
        #                             f'"{series}"), will turn this into a series')

        #         dfs[key] = pd.Series([dfs[key] for _ in range(len(good_series)) ])

        # for key,series in dfs.items():
        #     dfs[key] = series.rename(key)
            
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

        #check for duplicate indicies
        #for key,df in dfs.items():
        #    dups = df.index.duplicated()
        #    if len(df[dups])>1:
        #        self.logger.warning(f"{key} {len(df[dups])}/{len(df)} indicies (person_id) are duplicated")
        #        self.logger.warning(f"      if this is synthetic data... dont worry about it")
        #        dfs[key] = df[~df.index.duplicated()]
            
        #create a dataframe from all the series objects
        df = pd.concat(dfs.values(),axis=1)

        #find which fields in the cdm havent been defined
        missing_fields = set(self.fields) - set(df.columns)
        #set these to a nan/null series
        for field in missing_fields:
            df[field] = np.NaN

        #simply order the columns 
        df = df[self.fields]
        
        return df
