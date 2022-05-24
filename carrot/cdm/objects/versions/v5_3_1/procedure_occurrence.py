import pandas as pd
from ...common import DestinationTable, DestinationField

class ProcedureOccurrence(DestinationTable):
    """
    CDM Procedure Occurrence object class
    """
    
    name = 'procedure_occurrence'
    def __init__(self,name=None):
        self.procedure_occurrence_id     = DestinationField(dtype="Integer"   , required=False , pk=True)
        self.person_id                   = DestinationField(dtype="Integer"   , required=False )
        self.procedure_concept_id        = DestinationField(dtype="Integer"   , required=False )
        self.procedure_date              = DestinationField(dtype="Date"      , required=False )
        self.procedure_datetime          = DestinationField(dtype="Timestamp" , required=False )
        self.procedure_type_concept_id   = DestinationField(dtype="Integer"   , required=False )
        self.modifier_concept_id         = DestinationField(dtype="Integer"   , required=False )
        self.quantity                    = DestinationField(dtype="Integer"   , required=False )
        self.provider_id                 = DestinationField(dtype="Integer"   , required=False )
        self.visit_occurrence_id         = DestinationField(dtype="Integer"   , required=False )
        self.procedure_source_value      = DestinationField(dtype="Text50"    , required=False )
        self.procedure_source_concept_id = DestinationField(dtype="Integer"   , required=False )
        self.qualifier_source_value      = DestinationField(dtype="Text50"    , required=False )
        
        if name is None:
            name = hex(id(self))
        super().__init__(name,self.name)

            
    def get_df(self,**kwargs):
        """
        Overload/append the creation of the dataframe, specifically for the procedure_occurrence objects
        * procedure_concept_id is required to be not null
          this can happen when spawning multiple rows from a person
          we just want to keep the ones that have actually been filled
        
        Returns:
           pandas.Dataframe: output dataframe
        """

        df = super().get_df(**kwargs)
    
        if self.automatically_fill_missing_columns == True:
            if df['procedure_start_date'].isnull().all():
                df['procedure_start_date'] = self.tools.get_date(df['procedure_start_datetime'])

            if df['procedure_end_date'].isnull().all():
                df['procedure_end_date'] = self.tools.get_date(df['procedure_end_datetime'])
            
                
        return df
    
