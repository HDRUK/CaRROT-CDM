import pandas as pd
import datetime
from collections import OrderedDict

class ETLOperations(OrderedDict):
    """ETLOperations"""
    def get_year_from_date(self,df,**kwargs):
        """
        Convert a dataframe containing a datatime into the year only
        Args:
           df (pandas dataframe): the dataframe for the original source data
           **kwargs (dict) : keyword arguments, needed as this method gets called generically via allowed_operations
        Returns:
           pandas dataframe: with one column in datatime format only displaying the year 
        """
        #raise an error if we dont have column in the kwargs
        #column == field
        if 'column' not in kwargs:
            raise ValueError(f'column not found in kwargs: {kwargs}')
        series = df[kwargs['column']]
        return pd.to_datetime(series).dt.year#.fillna(0).astype(int)

        
    def get_month_from_date(self,df,**kwargs):
        """
        Convert a dataframe containing a datatime into the month only
        Args:
           df (pandas dataframe): the dataframe for the original source data
           **kwargs (dict) : keyword arguments, needed as this method gets called generically via allowed_operations
        Returns:
           pandas dataframe: with one column in datatime format only displaying the month 
        """
        #raise an error if we dont have column in the kwargs
        #column == field
        if 'column' not in kwargs:
            raise ValueError(f'column not found in kwargs: {kwargs}')
        series = df[kwargs['column']]
        return pd.to_datetime(series).dt.month#.fillna(0).astype(int)
    
    def get_day_from_date(self,df,**kwargs):
        """
        Convert a dataframe containing a datatime into the day only
        Args:
           df (pandas dataframe): the dataframe for the original source data
           **kwargs (dict) : keyword arguments, needed as this method gets called generically via allowed_operations
        Returns:
           pandas dataframe: with one column in datatime format only displaying the day 
        """
        #raise an error if we dont have column in the kwargs
        #column == field
        if 'column' not in kwargs:
            raise ValueError(f'column not found in kwargs: {kwargs}')
        series = df[kwargs['column']]
        return pd.to_datetime(series).dt.day#.fillna(0).astype(int)

    def age_to_datetime(self,df,**kwargs):
        if 'column' not in kwargs:
            raise ValueError(f'column not found in kwargs: {kwargs}')
        series = df[kwargs['column']]

        #set normalisation to middle of 2020
        norm = datetime.datetime(2020, 7, 1)

        series = series.fillna(0).apply(lambda x: norm - datetime.timedelta(days=365*x))

        return series
    
    def to_datetime(self,df,**kwargs):
        if 'column' not in kwargs:
            raise ValueError(f'column not found in kwargs: {kwargs}')
        series = df[kwargs['column']]
        series = pd.to_datetime(series)
        return series

    def __repr__(self):
        return "ETLOperations"
    
        
    def __init__(self):
        super().__init__(self)

        self['EXTRACT_YEAR'] = self.get_year_from_date
        self['EXTRACT_MONTH'] = self.get_month_from_date
        self['EXTRACT_DAY'] = self.get_day_from_date
        self['AGE_TO_DT'] = self.age_to_datetime
        self['TO_DT'] = self.to_datetime
        
        
        self.auto_functions = {
            'year_of_birth' : self['EXTRACT_YEAR'],
            'month_of_birth' : self['EXTRACT_MONTH'],
            'day_of_birth' : self['EXTRACT_DAY'],
            'birth_datetime': self['TO_DT']
        }

        
