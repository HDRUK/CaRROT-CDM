from ...common import DestinationTable, DestinationField

class Death(DestinationTable):
    """
    CDM Death object class
    """
    name = 'death'
    def __init__(self,name=None):
        self.person_id               = DestinationField(dtype="Integer"   , required=False , pk=True)
        self.death_date              = DestinationField(dtype="Date"      , required=False )
        self.death_datetime          = DestinationField(dtype="Timestamp" , required=False )
        self.death_type_concept_id   = DestinationField(dtype="Integer"   , required=False )
        self.cause_concept_id        = DestinationField(dtype="Integer"   , required=False )
        self.cause_source_value      = DestinationField(dtype="Text50"    , required=False )
        self.cause_source_concept_id = DestinationField(dtype="Integer"   , required=False )

        if name is None:
            name = hex(id(self))
        super().__init__(name,self.name)

    def finalise(self,df,**kwargs):
        """
        Overload the finalise function here for any specifics for the person table
        """
        df = super().finalise(df,**kwargs)
        return df


    def get_df(self,**kwargs):
        """
        Returns:
           pandas.Dataframe: output dataframe
        """
        df = super().get_df(**kwargs)
        if self.automatically_fill_missing_columns == True:
            pass
                
        return df
