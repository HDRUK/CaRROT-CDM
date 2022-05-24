import pandas as pd
from ...common import DestinationTable, DestinationField

class ConditionOccurrence(DestinationTable):
    """
    CDM Condition Occurrence object class
    """
    
    name = 'condition_occurrence'
    def __init__(self,name=None):
        self.condition_occurrence_id       = DestinationField(dtype="Integer"   , required=True , pk=True)
        self.person_id                     = DestinationField(dtype="Integer"   , required=True )
        self.condition_concept_id          = DestinationField(dtype="Integer"   , required=True )
        self.condition_start_date          = DestinationField(dtype="Date"      , required=False )
        self.condition_start_datetime      = DestinationField(dtype="Timestamp" , required=True )
        self.condition_end_date            = DestinationField(dtype="Date"      , required=False )
        self.condition_end_datetime        = DestinationField(dtype="Timestamp" , required=False )
        self.condition_type_concept_id     = DestinationField(dtype="Integer"   , required=False )
        self.stop_reason                   = DestinationField(dtype="Text20"    , required=False )
        self.provider_id                   = DestinationField(dtype="Integer"   , required=False )
        self.visit_occurrence_id           = DestinationField(dtype="Integer"   , required=False )
        self.condition_source_value        = DestinationField(dtype="Text50"    , required=False )
        self.condition_source_concept_id   = DestinationField(dtype="Integer"   , required=False )
        self.condition_status_source_value = DestinationField(dtype="Text50"    , required=False )
        self.condition_status_concept_id   = DestinationField(dtype="Integer"   , required=False )
        
        if name is None:
            name = hex(id(self))
        super().__init__(name,self.name)

        
    def get_df(self,**kwargs):
        """
        Overload/append the creation of the dataframe, specifically for the condition_occurrence objects
        * condition_concept_id is required to be not null
          this can happen when spawning multiple rows from a person
          we just want to keep the ones that have actually been filled
        
        Returns:
           pandas.Dataframe: output dataframe
        """

        df = super().get_df(**kwargs)
        if self.automatically_fill_missing_columns == True:
            if df['condition_start_date'].isnull().all():
                df['condition_start_date'] = self.tools.get_date(df['condition_start_datetime'])

            if df['condition_end_date'].isnull().all():
                df['condition_end_date'] = self.tools.get_date(df['condition_end_datetime'])
        return df
    
