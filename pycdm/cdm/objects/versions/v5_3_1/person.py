from ...common import DestinationTable, DestinationField

class Person(DestinationTable):
    """
    CDM Person object class
    """
    name = 'person'
    def __init__(self,name=None):
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

        if name is None:
            name = hex(id(self))
        super().__init__(name,self.name)

    def finalise(self,df,**kwargs):
        """
        Overload the finalise function here for any specifics for the person table
        """
        df = super().finalise(df,**kwargs)
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
        df = super().get_df(**kwargs)
        if self.automatically_fill_missing_columns == True:
            if df['year_of_birth'].isnull().all():
                df['year_of_birth'] = self.tools.get_year(df['birth_datetime'])

            if df['month_of_birth'].isnull().all():
                df['month_of_birth'] = self.tools.get_month(df['birth_datetime'])

            if df['day_of_birth'].isnull().all():
                df['day_of_birth'] = self.tools.get_day(df['birth_datetime'])
                
        return df
