from .base import Base

class ConditionOccurrence(Base):
    """
    CDM Condition Occurrence object class
    """
    
    name = 'condition_occurrence'
    def __init__(self):
        super().__init__(self.name)
    
    def finalise(self,df):
        """
        Overloads the finalise method defined in the Base class.
        For condition_occurrence, the _id of the condition is often not set
        Therefore if the series is null, then we just make an incremental index for the _id

        Returns:
          pandas.Dataframe : finalised pandas dataframe
        """

        df = super().finalise(df)
        df = df.sort_values('person_id')
        if df['condition_occurrence_id'].isnull().any():
            df['condition_occurrence_id'] = df.reset_index().index + 1

            
        return df
        
    def get_df(self):
        """
        Overload/append the creation of the dataframe, specifically for the condition_occurrence objects
        * condition_concept_id is required to be not null
          this can happen when spawning multiple rows from a person
          we just want to keep the ones that have actually been filled
        
        Returns:
           pandas.Dataframe: output dataframe
        """

        df = super().get_df()
        #require the condition_concept_id to be filled
        df = df[df['condition_concept_id'].notnull()]
        
        return df
