from .base import Base

class Person(Base):
    """
    CDM Person object class
    """
    name = 'person'
    def __init__(self):
        super().__init__(self.name)

    def finalise(self,df):
        df = super().finalise(df)
        return df.sort_values('person_id')
        
    def get_df(self,do_auto_conversion=False):
        """
        Overload/append the creation of the dataframe, specifically for the person objects
        * year_of_birth is automatically converted to a year (int)
        * month_of_birth is automatically converted to a month (int)
        * day_of_birth is automatically converted to a day (int)
        * birth_datetime is automatically coverted to a datatime
        

        Returns:
           pandas.Dataframe: output dataframe
        """
        df = super().get_df()


        #auto conversion
        if do_auto_conversion:
            if not df['birth_datetime'].isnull().any():
                if df['year_of_birth'].isnull().all():
                    df['year_of_birth'] = self.tools.get_year(df['birth_datetime'])
                if df['month_of_birth'].isnull().all():
                    df['month_of_birth'] = self.tools.get_month(df['birth_datetime'])
                if df['day_of_birth'].isnull().all():
                    df['day_of_birth'] = self.tools.get_day(df['birth_datetime'])
        
        
        return df
