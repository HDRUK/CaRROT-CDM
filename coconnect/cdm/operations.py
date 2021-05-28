import pandas as pd
import datetime

class OperationTools:

    get_datetime = lambda self,df : pd.to_datetime(df).dt.strftime('%Y-%m-%d %H:%M:%S')
    get_date = lambda self,df : pd.to_datetime(df).dt.strftime('%Y-%m-%d')
    get_year = lambda self,df : pd.to_datetime(df).dt.year.astype('Int64')
    get_month = lambda self,df : pd.to_datetime(df).dt.month.astype('Int64')
    get_day = lambda self,df : pd.to_datetime(df).dt.day.astype('Int64')

    def get_datetime_from_age(self,df,norm=None):
        if norm is None:
            #set normalisation to middle of 2020
            norm = datetime.datetime(2020, 7, 1)
        retval = df.fillna(0).apply(lambda x: norm - datetime.timedelta(days=365*int(x)))
        return self.get_datetime(retval)

    def get_source_field_name_as_value(self,series):
        temp = series.copy()
        temp.values[:] = series.name
        return temp
    
    def make_scalar(self,series,value):
        #if we dont do a copy, the original data gets replaced
        temp = series.copy()
        temp.values[:] = value
        return temp

    def __getitem__(self, item):
        try:
            retval = getattr(self,item)
        except AttributeError as err:
            raise UnknownOperation(err)
        return retval
