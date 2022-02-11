import pandas as pd
from ...common import DestinationTable, DestinationField

class Specimen(DestinationTable):
    """
    CDM Specimen object class
    """
    
    name = 'specimen'
    def __init__(self,name=None):

        self.specimen_id                 = DestinationField(dtype="Integer"   , required=False , pk=True)
        self.person_id                   = DestinationField(dtype="Integer"   , required=False )
        self.specimen_concept_id         = DestinationField(dtype="Integer"   , required=False )
        self.specimen_type_concept_id    = DestinationField(dtype="Integer"   , required=False )
        self.specimen_date               = DestinationField(dtype="Date"      , required=False )
        self.specimen_datetime           = DestinationField(dtype="Timestamp" , required=False )
        self.quantity                    = DestinationField(dtype="Float"     , required=False )
        self.unit_concept_id             = DestinationField(dtype="Integer"   , required=False )
        self.anatomic_site_concept_id    = DestinationField(dtype="Integer"   , required=False )
        self.disease_status_concept_id   = DestinationField(dtype="Integer"   , required=False )
        self.specimen_source_id          = DestinationField(dtype="Text50"    , required=False )
        self.specimen_source_value       = DestinationField(dtype="Text50"    , required=False )
        self.unit_source_value           = DestinationField(dtype="Text50"    , required=False )
        self.anatomic_site_source_value  = DestinationField(dtype="Text50"    , required=False )
        self.disease_status_source_value = DestinationField(dtype="Text50"    , required=False )
        
        if name is None:
            name = hex(id(self))
        super().__init__(name,self.name)

    def get_df(self,**kwargs):
        """
        Overload/append the creation of the dataframe, specifically for the specimen objects
        * specimen_concept_id is required to be not null
          this can happen when spawning multiple rows from a person
          we just want to keep the ones that have actually been filled
        
        Returns:
           pandas.Dataframe: output dataframe
        """

        df = super().get_df(**kwargs)

        if self.automatically_fill_missing_columns == True:
            if df['specimen_date'].isnull().all():
                df['specimen_date'] = self.tools.get_date(df['specimen_datetime'])
        return df
