from .base import Base, DataType

class Person(Base):
    """
    CDM Person object class
    """
    name = 'person'
    def __init__(self):
        self.person_id                   = DataType(dtype="INTEGER"     , required=True , pk=True)
        self.gender_concept_id           = DataType(dtype="INTEGER"     , required=True)
        self.year_of_birth               = DataType(dtype="INTEGER"     , required=False)
        self.month_of_birth              = DataType(dtype="INTEGER"     , required=False)
        self.day_of_birth                = DataType(dtype="INTEGER"     , required=False)
        self.birth_datetime              = DataType(dtype="DATETIME"    , required=True)
        self.race_concept_id             = DataType(dtype="INTEGER"     , required=False)
        self.ethnicity_concept_id        = DataType(dtype="INTEGER"     , required=False)
        self.location_id                 = DataType(dtype="INTEGER"     , required=False)
        self.provider_id                 = DataType(dtype="INTEGER"     , required=False)
        self.care_site_id                = DataType(dtype="INTEGER"     , required=False)
        self.person_source_value         = DataType(dtype="VARCHAR(50)" , required=False)
        self.gender_source_value         = DataType(dtype="VARCHAR(50)" , required=False)
        self.gender_source_concept_id    = DataType(dtype="INTEGER"     , required=False)
        self.race_source_value           = DataType(dtype="VARCHAR(50)" , required=False)
        self.race_source_concept_id      = DataType(dtype="INTEGER"     , required=False)
        self.ethnicity_source_value      = DataType(dtype="VARCHAR(50)" , required=False)
        self.ethnicity_source_concept_id = DataType(dtype="INTEGER"     , required=False)
        
        super().__init__(self.name)

    def finalise(self,df):
        """
        Overload the finalise function here for any specifics for the person table
        """
        df = super().finalise(df)
        #df = df.sort_values('person_id')
        return df


    def get_df(self):
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

        if self.automatically_generate_missing_rules == True:
            if df['year_of_birth'].isnull().all():
                df['year_of_birth'] = self.tools.get_year(df['birth_datetime'])
                
            if df['month_of_birth'].isnull().all():
                df['month_of_birth'] = self.tools.get_month(df['birth_datetime'])
                
            if df['day_of_birth'].isnull().all():
                df['day_of_birth'] = self.tools.get_day(df['birth_datetime'])
        
        return df
