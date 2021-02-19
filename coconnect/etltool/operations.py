import pandas as pd
from collections import OrderedDict

class ETLOperations(OrderedDict):
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
        #get the pandas series of the input dataframe (get the field of the input dataset) to be modified
        series = df[kwargs['column']]
        # - convert the series into a datetime series
        # - get the datetime object via .dt
        # - get only the year via the .dt object
        # - convert the series back into a pandas dataframe
        return pd.to_datetime(series).dt.year

    
    def __init__(self):
        super().__init__(self)
        print ('hiya!')

        self['EXTRACT_YEAR'] = self.get_year_from_date
              
