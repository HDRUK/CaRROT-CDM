from .base import Base

class Person(Base):
    name = 'person'
    def __init__(self):
        super().__init__(self.name)
                
    def get_df(self):
        df = super().get_df()
        #convert these key fields
        df['year_of_birth'] = self.tools.get_year(df['year_of_birth'])
        df['month_of_birth'] = self.tools.get_month(df['month_of_birth'])
        df['day_of_birth'] = self.tools.get_day(df['day_of_birth'])
        df['birth_datetime'] = self.tools.get_datetime(df['birth_datetime'])
        return df
