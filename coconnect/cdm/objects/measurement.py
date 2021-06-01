import pandas as pd
from .base import Base, DataType

class Measurement(Base):
    """
    CDM Measurement object class
    """
    
    name = 'measurement'
    def __init__(self):
        self.measurement_id                = DataType(dtype="INTEGER"     , required=True)
        self.person_id                     = DataType(dtype="INTEGER"     , required=True)
        self.measurement_concept_id        = DataType(dtype="INTEGER"     , required=True)
        self.measurement_date              = DataType(dtype="DATE"        , required=True)
        self.measurement_datetime          = DataType(dtype="DATETIME"    , required=False)
        self.measurement_time              = DataType(dtype="VARCHAR(10)" , required=False)
        self.measurement_type_concept_id   = DataType(dtype="INTEGER"     , required=True)
        self.operator_concept_id           = DataType(dtype="INTEGER"     , required=False)
        self.value_as_number               = DataType(dtype="FLOAT"       , required=False)
        self.value_as_concept_id           = DataType(dtype="INTEGER"     , required=False)
        self.unit_concept_id               = DataType(dtype="INTEGER"     , required=False)
        self.range_low                     = DataType(dtype="FLOAT"       , required=False)
        self.range_high                    = DataType(dtype="FLOAT"       , required=False)
        self.provider_id                   = DataType(dtype="INTEGER"     , required=False)
        self.visit_occurrence_id           = DataType(dtype="INTEGER"     , required=False)
        self.visit_detail_id               = DataType(dtype="INTEGER"     , required=False)
        self.measurement_source_value      = DataType(dtype="VARCHAR(50)" , required=False)
        self.measurement_source_concept_id = DataType(dtype="INTEGER"     , required=False)
        self.unit_source_value             = DataType(dtype="VARCHAR(50)" , required=False)
        self.value_source_value            = DataType(dtype="VARCHAR(50)" , required=False)

        super().__init__(self.name)


    @classmethod
    def finalise(cls,df):
        """
        Overloads the finalise method defined in the Base class.

        For measurement, the _id of the measurement is often not set

        Therefore if the series is null, then we just make an incremental index for the _id

        Returns:
          pandas.Dataframe : finalised pandas dataframe
        """
        df = df.sort_values('person_id')
        if df['measurement_id'].isnull().any():
            df['measurement_id'] = df.reset_index().index + 1
        return df
        
    def get_df(self):
        """
        Overload/append the creation of the dataframe, specifically for the measurement objects
        * measurement_concept_id is required to be not null
          this can happen when spawning multiple rows from a person
          we just want to keep the ones that have actually been filled
        
        Returns:
           pandas.Dataframe: output dataframe
        """

        df = super().get_df()

        #make sure the concept_ids are numeric, otherwise set them to null
        df['measurement_concept_id'] = pd.to_numeric(df['measurement_concept_id'],errors='coerce')

        #require the measurement_concept_id to be filled
        nulls = df['measurement_concept_id'].isnull()
        if nulls.all():
            self.logger.error("the measurement_concept_id for this instance is all null")
            self.logger.error("most likely because there is no term mapping applied")
            self.logger.error("automatic conversion to a numeric has failed")
            
        df = df[~nulls]
        return df
