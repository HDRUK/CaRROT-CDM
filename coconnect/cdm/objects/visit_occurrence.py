import pandas as pd
from .common import DestinationTable, DestinationField

class VisitOccurrence(DestinationTable):
    """
    CDM Visit Occurrence object class
    """
    
    name = 'visit_occurrence'
    def __init__(self):
        self.visit_occurrence_id           = DestinationField(dtype="Integer"   , required=True, pk=True)
        self.person_id                     = DestinationField(dtype="Integer"   , required=True)
        self.visit_concept_id              = DestinationField(dtype="Integer"   , required=True)
        self.visit_start_date              = DestinationField(dtype="Date"      , required=True)
        self.visit_start_datetime          = DestinationField(dtype="Timestamp" , required=False)
        self.visit_end_date                = DestinationField(dtype="Date"      , required=True)
        self.visit_end_datetime            = DestinationField(dtype="Timestamp" , required=False)
        self.visit_type_concept_id         = DestinationField(dtype="Integer"   , required=True)
        self.provider_id                   = DestinationField(dtype="Integer"   , required=False)
        self.care_site_id                  = DestinationField(dtype="Integer"   , required=False)
        self.visit_source_value            = DestinationField(dtype="Text50"    , required=False)
        self.visit_source_concept_id       = DestinationField(dtype="Integer"   , required=False)
        self.admitting_source_concept_id   = DestinationField(dtype="Integer"   , required=False)
        self.admitting_source_value        = DestinationField(dtype="Text50"    , required=False)
        self.discharge_to_concept_id       = DestinationField(dtype="Integer"   , required=False)
        self.discharge_to_source_value     = DestinationField(dtype="Text50"    , required=False)
        self.preceding_visit_occurrence_id = DestinationField(dtype="Integer"   , required=False)
        
        super().__init__(self.name)


    def finalise(self,df):
        """
        Overloads the finalise method defined in the DestinationTable class.
        For visit_occurrence, the _id of the visit is often not set
        Therefore if the series is null, then we just make an incremental index for the _id

        Returns:
          pandas.Dataframe : finalised pandas dataframe
        """
#if the _id is all null, give them a temporary index
        #so that all rows are not removed when performing the check on
        #the required rows being filled 
        if df['visit_occurrence_id'].isnull().any():
            df['visit_occurrence_id'] = df.reset_index().index + 1
            
        df = super().finalise(df)
        #since the above finalise() will drop some rows, reset the index again
        #this just resets the _ids to be 1,2,3,4,5 instead of 1,2,5,6,8,10...
        df['visit_occurrence_id'] = df.reset_index().index + 1

        return df
