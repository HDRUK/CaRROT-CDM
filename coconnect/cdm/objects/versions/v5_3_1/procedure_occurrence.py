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

        
    def finalise(self,df):
        """
        Overloads the finalise method defined in the DestinationTable class.
        For procedure_occurrence, the _id of the procedure is often not set
        Therefore if the series is null, then we just make an incremental index for the _id

        Returns:
          pandas.Dataframe : finalised pandas dataframe
        """

        #if the _id is all null, give them a temporary index
        #so that all rows are not removed when performing the check on
        #the required rows being filled 
        if df['procedure_occurrence_id'].isnull().any():
            df['procedure_occurrence_id'] = df.reset_index().index + 1
            
        df = super().finalise(df)
        #since the above finalise() will drop some rows, reset the index again
        #this just resets the _ids to be 1,2,3,4,5 instead of 1,2,5,6,8,10...
        df['procedure_occurrence_id'] = df.reset_index().index + 1
        return df
        
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
        #make sure the concept_ids are numeric, otherwise set them to null
        df['procedure_concept_id'] = pd.to_numeric(df['procedure_concept_id'],errors='coerce')
        #require the procedure_concept_id to be filled
        nulls = df['procedure_concept_id'].isnull()
        if nulls.all() and len(df['procedure_concept_id']) > 0 :
            self.logger.error("the procedure_concept_id for this instance is all null")
            self.logger.error("most likely because there is no term mapping applied")
            self.logger.error("automatic conversion to a numeric has failed")

        if self.automatically_fill_missing_columns == True:
            if df['procedure_start_date'].isnull().all():
                df['procedure_start_date'] = self.tools.get_date(df['procedure_start_datetime'])

            if df['procedure_end_date'].isnull().all():
                df['procedure_end_date'] = self.tools.get_date(df['procedure_end_datetime'])
            

            
        df = df[~nulls]
                
        return df
    
