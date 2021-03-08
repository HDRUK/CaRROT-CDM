from .base import Base

class ConditionOccurrence(Base):
    name = 'condition_occurrence'
    def __init__(self):
        super().__init__(self.name)
        

    def finalise(self,df):
        df = df.sort_values('person_id')
        if df['condition_occurrence_id'].isnull().any():
            df['condition_occurrence_id'] = df.reset_index(drop=True).reset_index()['index']
                        
        return df
        
    def get_df(self):
        df = super().get_df()
        #require the condition_concept_id to be filled
        df = df[df['condition_concept_id'].notnull()]

        return df
