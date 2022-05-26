import pandas as pd
from ...common import DestinationTable, DestinationField

class VisitOccurrence(DestinationTable):
    """
    CDM Visit Occurrence object class
    """
    
    name = 'visit_occurrence'
    def __init__(self,name=None):
        self.visit_occurrence_id           = DestinationField(dtype="Integer"   , required=True, pk=True)
        self.person_id                     = DestinationField(dtype="Integer"   , required=True)
        self.visit_concept_id              = DestinationField(dtype="Integer"   , required=True)
        self.visit_start_date              = DestinationField(dtype="Date"      , required=True)
        self.visit_start_datetime          = DestinationField(dtype="Timestamp" , required=False)
        self.visit_end_date                = DestinationField(dtype="Date"      , required=True)
        self.visit_end_datetime            = DestinationField(dtype="Timestamp" , required=False)
        self.visit_type_concept_id         = DestinationField(dtype="Integer"   , required=True)
        self.provider_id                   = DestinationField(dtype="Integer"   , required=False)
        self.care_site_id                  = DestinationField(dtype="Integer"   , required=False)
        self.visit_source_value            = DestinationField(dtype="Text50"    , required=False)
        self.visit_source_concept_id       = DestinationField(dtype="Integer"   , required=False)
        self.admitting_source_concept_id   = DestinationField(dtype="Integer"   , required=False)
        self.admitting_source_value        = DestinationField(dtype="Text50"    , required=False)
        self.discharge_to_concept_id       = DestinationField(dtype="Integer"   , required=False)
        self.discharge_to_source_value     = DestinationField(dtype="Text50"    , required=False)
        self.preceding_visit_occurrence_id = DestinationField(dtype="Integer"   , required=False)
        
        if name is None:
            name = hex(id(self))
        super().__init__(name,self.name)
