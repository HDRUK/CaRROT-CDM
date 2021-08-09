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

class FormattingError(Exception):
    pass

class BadInputs(Exception):
    pass

class DataFormatter(collections.OrderedDict):
    """
    Class for formatting DestinationFields in the CommonDataModel

    Inherits from an ordered dictionary, and maps datatypes to lambda functions.
    The lamba functions encode how to transform and format a pandas series given the datatype.

    """
    def __init__(self):
        super().__init__()
        self['Integer'] = lambda x : pd.to_numeric(x,errors='coerce').astype('Int64')
        self['Float'] = lambda x : pd.to_numeric(x,errors='coerce').astype('Float64')
        self['Text20'] = lambda x : x.fillna('').astype(str).apply(lambda x: x[:20])
        self['Text50'] = lambda x : x.fillna('').astype(str).apply(lambda x: x[:50])
        self['Text60'] = lambda x : x.fillna('').astype(str).apply(lambda x: x[:60])
        self['Timestamp'] = lambda x : pd.to_datetime(x,errors='coerce').dt.strftime('%Y-%m-%d %H:%M:%S')
        self['Date'] = lambda x : pd.to_datetime(x,errors='coerce').dt.date


class DestinationField(object):
    """
    CommonDataModel Table Destination Field.

    Object for handling output columns (destination fields) in a 
    Destination Table

    Attributes:
       series (pandas.Series): raw column data in the form of a series
       dtype (str): data type for how to format the column based on the DataFormatter
       required (bool): if the column is required or not 
                        i.e. if the row should be delete if it is not filled
       pk (str): primary key label, indicating if the column is the primary required field

    """
    def __init__(self, dtype: str, required: bool, pk=False):
        self.series = None
        self.dtype = dtype
        self.required = required
        self.pk = pk

class DestinationTable(object):
    """
    Common object that all CDM objects (tables) inherit from.
    """
    def __init__(self,_type,_version='v5_3_1'):
        """
        Initialise the CDM DestinationTable Object class
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

        #get the required fields
        self.required_fields = [
            field
            for field in self.get_field_names()
            if getattr(self,field).required == True
        ]


    def get_field_names(self):
        """
        From the current object, loop over all member objects and find those that are instances
        of a DestinationField (column)
        
        Returns:
           list : a list of destination fields (columns [series])

        """
        return [
            item
            for item in self.__dict__.keys()
            if isinstance(getattr(self,item),DestinationField)
        ]

    def get_ordering(self):
        """
        Loops over all associated fields and finds which have been marked as being a primary key.

        Returns:
            list: a string list of the names of primary columns (fields)
        """
        retval = [
            field
            for field in self.fields
            if getattr(self,field).pk == True
        ]

        if len(retval) == 0:
            #warning, no pk has been set on any field
            retval = self.fields[0]
        
        return retval
        
    def __getitem__(self, key):
        """
        Retrieve a field (column) from the table (dataframe)

        Args:
           key (str) : name of a destination field
        Returns:
           DestinationField : the destination field object
        """
        
        return getattr(self, key)

    def __setitem__(self, key, obj):
        """
        Register a field object with the table
        """
        return setattr(self, key, obj)
    
    def set_name(self,name):
        """
        Register/Set the name of the destination table
        """
        self.name = name
        self.logger.name = self.name
    
    def define(self,_):
        """
        define function, expected to be overloaded by the user defining the object
        """
        pass

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

        #execute the define function
        #the default define() does nothing
        #this is only executed if the CDM has been build via decorators
        #or define functions have been specified for this object
        # it will build the inputs from these functions
        self.define(self)

        #build the dataframe for this object
        _ = self.get_df()
        
    def get_df(self,force_rebuild=True):
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

        df = self.format(df)
        df = self.finalise(df)
                
        #register the df
        self.__df = df
        return df

    def format(self,df):
        for col in df.columns:
            
            #if is already all na/nan, dont bother trying to format
            if df[col].isna().all():
                continue
            
            obj = getattr(self,col)
            dtype = obj.dtype
            formatter_function = self.dtypes[dtype]

            nbefore = len(df[col])
            nsample = 5 if nbefore > 5 else nbefore
            sample = df[col].sample(nsample)
            df[col] = formatter_function(df[col])

            if col in self.required_fields and df[col].isna().all():
                self.logger.error(f"Something wrong with the formatting of the required field {col} using {dtype}")
                self.logger.info(f"Sample of this column before formatting:")
                self.logger.error(sample)

                raise FormattingError(f"When formatting the required column {col}, using the formatter function {dtype}, all produced values are  NaN/null values.")

        return df

    def finalise(self,df):
        """
        Finalise a dataframe by dropping null/nan rows if a required field is missing.
        also sort the dataframe by the primary key of the table.

        Args:
            df (pandas.Dataframe): input dataframe
        Returns:
            pandas.Dataframe: cleaned output dataframe
        """

        self._meta['required_fields'] = {}

        #loop over the required fields
        for field in self.required_fields:
            #count the number of rows before
            nbefore = len(df)
            #remove rows which do not have this required field filled
            df = df[~df[field].isna()]
            #count the number of rows after
            nafter = len(df)
            #get the number of rows removed
            ndiff = nbefore - nafter
            #if rows have been removed
            if ndiff>0:
                #log a warning message if after requiring non-NaN values has removed all rows
                log = self.logger.warning if nafter > 0 else self.logger.error
                log(f"Requiring non-null values in {field} removed {ndiff} rows, leaving {nafter} rows.")

            #log some metadata
            self._meta['required_fields'][field] = {
                'before':nbefore,
                'after':nafter
            }

        #return the dataframe sorted by the primary key requested
        df = df.sort_values(self.get_ordering())
        return df

