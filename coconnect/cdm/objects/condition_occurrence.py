import pandas as pd
from .common import DestinationTable, DestinationField

class ConditionOccurrence(DestinationTable):
    """
    CDM Condition Occurrence object class
    """
    
    name = 'condition_occurrence'
    def __init__(self):
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
        
        super().__init__(self.name)
        
        
    def finalise(self,df):
        """
        Overloads the finalise method defined in the DestinationTable class.
        For condition_occurrence, the _id of the condition is often not set
        Therefore if the series is null, then we just make an incremental index for the _id

        Returns:
          pandas.Dataframe : finalised pandas dataframe
        """

        #if the _id is all null, give them a temporary index
        #so that all rows are not removed when performing the check on
        #the required rows being filled 
        if df['condition_occurrence_id'].isnull().any():
            df['condition_occurrence_id'] = df.reset_index().index + 1
            
        df = super().finalise(df)
        #since the above finalise() will drop some rows, reset the index again
        #this just resets the _ids to be 1,2,3,4,5 instead of 1,2,5,6,8,10...
        df['condition_occurrence_id'] = df.reset_index().index + 1
        return df
        
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
        #make sure the concept_ids are numeric, otherwise set them to null
        df['condition_concept_id'] = pd.to_numeric(df['condition_concept_id'],errors='coerce')
        #require the condition_concept_id to be filled
        nulls = df['condition_concept_id'].isnull()
        if nulls.all() and len(df['condition_concept_id']) > 0 :
            self.logger.error("the condition_concept_id for this instance is all null")
            self.logger.error("most likely because there is no term mapping applied")
            self.logger.error("automatic conversion to a numeric has failed")

        df = df[~nulls]
                
        return df
    
