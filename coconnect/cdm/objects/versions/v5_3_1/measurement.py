import pandas as pd
from ...common import DestinationTable, DestinationField

class Measurement(DestinationTable):
    """
    CDM Measurement object class
    """
    
    name = 'measurement'
    def __init__(self,name=None):
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

        if name is None:
            name = hex(id(self))
        super().__init__(name,self.name)

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
        if self.automatically_fill_missing_columns == True:
            if df['measurement_date'].isnull().all():
                df['measurement_date'] = self.tools.get_date(df['measurement_datetime'])
            if df['value_as_number'].isnull().all():
                df['value_as_number'] = pd.to_numeric(df['measurement_source_value'],errors='coerce').astype('Float64')
        
        return df
