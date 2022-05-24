import pandas as pd
from ...common import DestinationTable, DestinationField

class Observation(DestinationTable):
    """
    CDM Observation object class
    """
    
    name = 'observation'
    def __init__(self,name=None):
        self.observation_id                = DestinationField(dtype="Integer"   , required=True , pk=True)
        self.person_id                     = DestinationField(dtype="Integer"   , required=True )
        self.observation_concept_id        = DestinationField(dtype="Integer"   , required=True )
        self.observation_date              = DestinationField(dtype="Date"      , required=False )
        self.observation_datetime          = DestinationField(dtype="Timestamp" , required=True )
        self.observation_type_concept_id   = DestinationField(dtype="Integer"   , required=False )
        self.value_as_number               = DestinationField(dtype="Float"     , required=False )
        self.value_as_string               = DestinationField(dtype="Text60"    , required=False )
        self.value_as_concept_id           = DestinationField(dtype="Integer"   , required=False )
        self.qualifier_concept_id          = DestinationField(dtype="Integer"   , required=False )
        self.unit_concept_id               = DestinationField(dtype="Integer"   , required=False )
        self.provider_id                   = DestinationField(dtype="Integer"   , required=False )
        self.visit_occurrence_id           = DestinationField(dtype="Integer"   , required=False )
        self.observation_source_value      = DestinationField(dtype="Text50"    , required=False )
        self.observation_source_concept_id = DestinationField(dtype="Integer"   , required=False )
        self.unit_source_value             = DestinationField(dtype="Text50"    , required=False )
        self.qualifier_source_value        = DestinationField(dtype="Text50"    , required=False )

        if name is None:
            name = hex(id(self))
        super().__init__(name,self.name)


    def get_df(self,**kwargs):
        """
        Overload/append the creation of the dataframe, specifically for the observation objects
        * observation_concept_id is required to be not null
          this can happen when spawning multiple rows from a person
          we just want to keep the ones that have actually been filled
        
        Returns:
           pandas.Dataframe: output dataframe
        """

        df = super().get_df(**kwargs)
        if self.automatically_fill_missing_columns == True:
            if df['observation_date'].isnull().all():
                df['observation_date'] = self.tools.get_date(df['observation_datetime'])

        return df
