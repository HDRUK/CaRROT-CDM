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
        
    def get_df(self):
        """
        Overload/append the creation of the dataframe, specifically for the person objects
        * year_of_birth is automatically converted to a year (int)
        * month_of_birth is automatically converted to a month (int)
        * day_of_virth is automatically converted to a day (int)
        * birth_datetime is automatically coverted to a datatime
        

        Returns:
           pandas.Dataframe: output dataframe
        """
        df = super().get_df()

        #convert these key fields
        df['year_of_birth'] = self.tools.get_year(df['year_of_birth'])
        df['month_of_birth'] = self.tools.get_month(df['month_of_birth'])
        df['day_of_birth'] = self.tools.get_day(df['day_of_birth'])
        df['birth_datetime'] = self.tools.get_datetime(df['birth_datetime'])

        
        return df
