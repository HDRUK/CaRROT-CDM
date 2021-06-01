import pandas as pd
from .base import Base, DataType

class VisitOccurrence(Base):
    """
    CDM Visit Occurrence object class
    """
    
    name = 'visit_occurrence'
    def __init__(self):
        super().__init__(self.name)
        self.visit_occurrence_id           = DataType(dtype="INTEGER"     , required=True)
        self.person_id                     = DataType(dtype="INTEGER"     , required=True)
        self.visit_concept_id              = DataType(dtype="INTEGER"     , required=True)
        self.visit_start_date              = DataType(dtype="DATE"        , required=True)
        self.visit_start_datetime          = DataType(dtype="DATETIME"    , required=False)
        self.visit_end_date                = DataType(dtype="DATE"        , required=True)
        self.visit_end_datetime            = DataType(dtype="DATETIME"    , required=False)
        self.visit_type_concept_id         = DataType(dtype="INTEGER"     , required=True)
        self.provider_id                   = DataType(dtype="INTEGER"     , required=False)
        self.care_site_id                  = DataType(dtype="INTEGER"     , required=False)
        self.visit_source_value            = DataType(dtype="VARCHAR(50)" , required=False)
        self.visit_source_concept_id       = DataType(dtype="INTEGER"     , required=False)
        self.admitting_source_concept_id   = DataType(dtype="INTEGER"     , required=False)
        self.admitting_source_value        = DataType(dtype="VARCHAR(50)" , required=False)
        self.discharge_to_concept_id       = DataType(dtype="INTEGER"     , required=False)
        self.discharge_to_source_value     = DataType(dtype="VARCHAR(50)" , required=False)
        self.preceding_visit_occurrence_id = DataType(dtype="INTEGER"     , required=False)
        
    def finalise(self,df):
        """
        Overloads the finalise method defined in the Base class.
        For visit_occurrence, the _id of the visit is often not set
        Therefore if the series is null, then we just make an incremental index for the _id

        Returns:
          pandas.Dataframe : finalised pandas dataframe
        """

        df = super().finalise(df)
        df = df.sort_values('person_id')
        if df['visit_occurrence_id'].isnull().any():
            df['visit_occurrence_id'] = df.reset_index().index + 1

            
        return df
        
    def get_df(self):
        """
        Overload/append the creation of the dataframe, specifically for the visit_occurrence objects
        * visit_concept_id is required to be not null
          this can happen when spawning multiple rows from a person
          we just want to keep the ones that have actually been filled
        
        Returns:
           pandas.Dataframe: output dataframe
        """

        df = super().get_df()

        #make sure the concept_ids are numeric, otherwise set them to null
        #df['condition_concept_id'] = pd.to_numeric(df['condition_concept_id'],errors='coerce')

        #require the condition_concept_id to be filled
        #nulls = df['condition_concept_id'].isnull()
        #if nulls.all():
        #    self.logger.error("the condition_concept_id for this instance is all null")
        #    self.logger.error("most likely because there is no term mapping applied")
        #    self.logger.error("automatic conversion to a numeric has failed")
            
        #df = df[~nulls]
        
        return df
