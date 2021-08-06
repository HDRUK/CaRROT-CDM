import pandas as pd
from .common import DestinationTable, DestinationField

class Measurement(DestinationTable):
    """
    CDM Measurement object class
    """
    
    name = 'measurement'
    def __init__(self):
        self.measurement_id                = DestinationField(dtype="Integer"   , required=True  , pk=True)
        self.person_id                     = DestinationField(dtype="Integer"   , required=True )
        self.measurement_concept_id        = DestinationField(dtype="Integer"   , required=True )
        self.measurement_date              = DestinationField(dtype="Date"      , required=False )
        self.measurement_datetime          = DestinationField(dtype="Timestamp" , required=True )
        self.measurement_type_concept_id   = DestinationField(dtype="Integer"   , required=False )
        self.operator_concept_id           = DestinationField(dtype="Integer"   , required=False )
        self.value_as_number               = DestinationField(dtype="Float"     , required=False )
        self.value_as_concept_id           = DestinationField(dtype="Integer"   , required=False )
        self.unit_concept_id               = DestinationField(dtype="Integer"   , required=False )
        self.range_low                     = DestinationField(dtype="Float"     , required=False )
        self.range_high                    = DestinationField(dtype="Float"     , required=False )
        self.provider_id                   = DestinationField(dtype="Integer"   , required=False )
        self.visit_occurrence_id           = DestinationField(dtype="Integer"   , required=False )
        self.measurement_source_value      = DestinationField(dtype="Text50"    , required=False )
        self.measurement_source_concept_id = DestinationField(dtype="Integer"   , required=False )
        self.unit_source_value             = DestinationField(dtype="Text50"    , required=False )
        self.value_source_value            = DestinationField(dtype="Text50"    , required=False )


        super().__init__(self.name)


    def finalise(self,df):
        """
        Overloads the finalise method defined in the DestinationTable class.

        For measurement, the _id of the measurement is often not set

        Therefore if the series is null, then we just make an incremental index for the _id

        Returns:
          pandas.Dataframe : finalised pandas dataframe
        """
        #if the _id is all null, give them a temporary index
        #so that all rows are not removed when performing the check on
        #the required rows being filled 
        if df['measurement_id'].isnull().any():
            df['measurement_id'] = df.reset_index().index + 1
            
        df = super().finalise(df)
        #since the above finalise() will drop some rows, reset the index again
        #this just resets the _ids to be 1,2,3,4,5 instead of 1,2,5,6,8,10...
        df['measurement_id'] = df.reset_index().index + 1

        return df
        
    def get_df(self,**kwargs):
        """
        Overload/append the creation of the dataframe, specifically for the measurement objects
        * measurement_concept_id is required to be not null
          this can happen when spawning multiple rows from a person
          we just want to keep the ones that have actually been filled
        
        Returns:
           pandas.Dataframe: output dataframe
        """

        df = super().get_df(**kwargs)

        #make sure the concept_ids are numeric, otherwise set them to null
        df['measurement_concept_id'] = pd.to_numeric(df['measurement_concept_id'],errors='coerce')

        #require the measurement_concept_id to be filled
        nulls = df['measurement_concept_id'].isnull()
        if nulls.all() and len(df['measurement_concept_id']) > 0 :
            self.logger.error("the measurement_concept_id for this instance is all null")
            self.logger.error("most likely because there is no term mapping applied")
            self.logger.error("automatic conversion to a numeric has failed")
            
        df = df[~nulls]
        return df
