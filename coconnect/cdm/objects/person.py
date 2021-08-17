from .common import DestinationTable, DestinationField

class Person(DestinationTable):
    """
    CDM Person object class
    """
    name = 'person'
    def __init__(self):
        self.person_id                   = DestinationField(dtype="Integer"   , required=True , pk=True)
        self.gender_concept_id           = DestinationField(dtype="Integer"   , required=True )
        self.year_of_birth               = DestinationField(dtype="Integer"   , required=False )
        self.month_of_birth              = DestinationField(dtype="Integer"   , required=False )
        self.day_of_birth                = DestinationField(dtype="Integer"   , required=False )
        self.birth_datetime              = DestinationField(dtype="Timestamp" , required=True )
        self.race_concept_id             = DestinationField(dtype="Integer"   , required=False )
        self.ethnicity_concept_id        = DestinationField(dtype="Integer"   , required=False )
        self.location_id                 = DestinationField(dtype="Integer"   , required=False )
        self.provider_id                 = DestinationField(dtype="Integer"   , required=False )
        self.care_site_id                = DestinationField(dtype="Integer"   , required=False )
        self.person_source_value         = DestinationField(dtype="Text50"    , required=False )
        self.gender_source_value         = DestinationField(dtype="Text50"    , required=False )
        self.gender_source_concept_id    = DestinationField(dtype="Integer"   , required=False )
        self.race_source_value           = DestinationField(dtype="Text50"    , required=False )
        self.race_source_concept_id      = DestinationField(dtype="Integer"   , required=False )
        self.ethnicity_source_value      = DestinationField(dtype="Text50"    , required=False )
        self.ethnicity_source_concept_id = DestinationField(dtype="Integer"   , required=False )
                
        super().__init__(self.name)

    def finalise(self,df):
        """
        Overload the finalise function here for any specifics for the person table
        """
        df = super().finalise(df)
        return df


    def get_df(self,**kwargs):
        """
        Overload/append the creation of the dataframe, specifically for the person objects
        * year_of_birth is automatically converted to a year (int)
        * month_of_birth is automatically converted to a month (int)
        * day_of_birth is automatically converted to a day (int)
        * birth_datetime is automatically coverted to a datatime
        

        Returns:
           pandas.Dataframe: output dataframe
        """
        
        if 'fill_missing_columns' in kwargs:
            self.fill_missing_columns = kwargs['fill_missing_columns']

        if hasattr(self,'fill_missing_columns'):
            if self.fill_missing_columns == True:
                kwargs['force_rebuild'] = True

                birth_datetime = self['birth_datetime'].series
                year_of_birth = self['year_of_birth'].series
                if year_of_birth.isnull().all():
                    self['year_of_birth'].series = self.tools.get_year(birth_datetime)

                month_of_birth = self['month_of_birth'].series
                if month_of_birth.isnull().all():
                    self['month_of_birth'].series = self.tools.get_month(birth_datetime)
                
                day_of_birth = self['day_of_birth'].series
                if day_of_birth.isnull().all():
                    self['day_of_birth'].series = self.tools.get_day(birth_datetime)
        
        df = super().get_df(**kwargs)
        return df
