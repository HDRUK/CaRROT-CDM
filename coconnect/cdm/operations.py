import pandas as pd
import datetime

class OperationTools:

    get_datetime = lambda self,df : pd.to_datetime(df).dt.strftime('%Y-%m-%d %H:%M:%S')
    get_date = lambda self,df : pd.to_datetime(df).dt.strftime('%Y-%m-%d')
    get_year = lambda self,df : pd.to_datetime(df).dt.year.astype('Int64').astype(str)
    get_month = lambda self,df : pd.to_datetime(df).dt.month.astype('Int64').astype(str)
    get_day = lambda self,df : pd.to_datetime(df).dt.day.astype('Int64').astype(str)

    def age_to_datetime(self,df,norm=None):
        if norm is None:
            #set normalisation to middle of 2020
            norm = datetime.datetime(2020, 7, 1)
        return df.fillna(0).apply(lambda x: norm - datetime.timedelta(days=365*int(x)))

    
    def map_term(self,df,term_map):
        fname = df.fname
        name = df.name
        mapper = {
            key:values['concept_id']
            for key,values in term_map[fname][name].items()
        }

        return df.map(mapper)

    def make_scalar(self,series,value):
        df = series.to_frame()
        df[~df.isnull()] = value
        return df[value]
