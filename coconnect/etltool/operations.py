import pandas as pd
from collections import OrderedDict

class ETLOperations(OrderedDict):


    
    def get_from_date(self,series,switch):
        """
        Convert a dataframe containing a datatime into the day/month/year only
        Args:
           series (pandas.series): the series for columnn for the original source data
           switch (str): switch between what to return 
        Returns:
           pandas.dataframe: with one column in datatime format only displaying the day/month/year
        """

        # - convert the series into a datetime series
        # - get the datetime object via .dt
        # - get only the month via the .dt object
        # - convert the series back into a pandas dataframe
        if switch == 'day':
            return pd.to_datetime(series).dt.day.to_frame()
        elif switch == 'month':
            return pd.to_datetime(series).dt.month.to_frame()
        elif switch == 'year':
            return pd.to_datetime(series).dt.year.to_frame()
        else:
            raise ValueError(f'no switch defined for {switch}')

    
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
        return self.get_from_date(series,'year')
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
        return self.get_from_date(series,'month')

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
        return self.get_from_date(series,'day')

    
    def __init__(self):
        super().__init__(self)
        print ('hiya!')

        self['EXTRACT_YEAR'] = self.get_year_from_date
        self['EXTRACT_MONTH'] = self.get_year_from_date
        self['EXTRACT_DAY'] = self.get_year_from_date
              
