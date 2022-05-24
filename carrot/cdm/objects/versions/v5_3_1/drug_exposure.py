import pandas as pd
from ...common import DestinationTable, DestinationField

class DrugExposure(DestinationTable):
    """
    CDM Drug Exposure object class
    """
    
    name = 'drug_exposure'
    def __init__(self,name=None):
        self.drug_exposure_id             = DestinationField(dtype="Integer"   , required=True , pk=True)
        self.person_id                    = DestinationField(dtype="Integer"   , required=True )
        self.drug_concept_id              = DestinationField(dtype="Integer"   , required=True )
        self.drug_exposure_start_date     = DestinationField(dtype="Date"      , required=False )
        self.drug_exposure_start_datetime = DestinationField(dtype="Timestamp" , required=True )
        self.drug_exposure_end_date       = DestinationField(dtype="Date"      , required=False )
        self.drug_exposure_end_datetime   = DestinationField(dtype="Timestamp" , required=False )
        self.verbatim_end_date            = DestinationField(dtype="Date"      , required=False )
        self.drug_type_concept_id         = DestinationField(dtype="Integer"   , required=False )
        self.stop_reason                  = DestinationField(dtype="Text20"    , required=False )
        self.refills                      = DestinationField(dtype="Integer"   , required=False )
        self.quantity                     = DestinationField(dtype="Float"     , required=False )
        self.days_supply                  = DestinationField(dtype="Integer"   , required=False )
        self.sig                          = DestinationField(dtype="Integer"   , required=False )
        self.route_concept_id             = DestinationField(dtype="Integer"   , required=False )
        self.lot_number                   = DestinationField(dtype="Text50"    , required=False )
        self.provider_id                  = DestinationField(dtype="Integer"   , required=False )
        self.visit_occurrence_id          = DestinationField(dtype="Integer"   , required=False )
        self.drug_source_value            = DestinationField(dtype="Text50"    , required=False )
        self.drug_source_concept_id       = DestinationField(dtype="Integer"   , required=False )
        self.route_source_value           = DestinationField(dtype="Text50"    , required=False )
        self.dose_unit_source_value       = DestinationField(dtype="Text50"    , required=False )
        
        if name is None:
            name = hex(id(self))
        super().__init__(name,self.name)

        
    def get_df(self,**kwargs):
        """
        Overload/append the creation of the dataframe, specifically for the drug_exposure objects
        * drug_concept_id  is required to be not null
          this can happen when spawning multiple rows from a person
          we just want to keep the ones that have actually been filled
        
        Returns:
           pandas.Dataframe: output dataframe
        """

        df = super().get_df(**kwargs)
        if self.automatically_fill_missing_columns == True:
            if df['drug_exposure_start_date'].isnull().all():
                df['drug_exposure_start_date'] = self.tools.get_date(df['drug_exposure_start_datetime'])

            if df['drug_exposure_end_date'].isnull().all():
                df['drug_exposure_end_date'] = self.tools.get_date(df['drug_exposure_end_datetime'])
        return df
    
