import pandas as pd
from .base import Base

class Observation(Base):
    """
    CDM Observation object class
    """
    
    name = 'observation'
    def __init__(self):
        super().__init__(self.name)
    
    def finalise(self,df):
        """
        Overloads the finalise method defined in the Base class.

        For observation, the _id of the observation is often not set

        Therefore if the series is null, then we just make an incremental index for the _id

        Returns:
          pandas.Dataframe : finalised pandas dataframe
        """

        df = super().finalise(df)
        df = df.sort_values('person_id')
        if df['observation_id'].isnull().any():
            df['observation_id'] = df.reset_index().index + 1

            
        return df
        
    def get_df(self):
        """
        Overload/append the creation of the dataframe, specifically for the observation objects
        * observation_concept_id is required to be not null
          this can happen when spawning multiple rows from a person
          we just want to keep the ones that have actually been filled
        
        Returns:
           pandas.Dataframe: output dataframe
        """

        df = super().get_df()

        #make sure the concept_ids are numeric, otherwise set them to null
        print (df['observation_concept_id'] )
        df['observation_concept_id'] = pd.to_numeric(df['observation_concept_id'],errors='coerce')

        #require the observation_concept_id to be filled
        nulls = df['observation_concept_id'].isnull()
        if nulls.all():
            self.logger.error("the observation_concept_id for this instance is all null")
            self.logger.error("most likely because there is no term mapping applied")
            self.logger.error("automatic conversion to a numeric has failed")
            
        df = df[~nulls]
        return df
