from .person import Person
from .condition_occurrence import ConditionOccurrence
from .visit_occurrence import VisitOccurrence
from .measurement import Measurement
from .observation import Observation
from .drug_exposure import DrugExposure

import sys
this = sys.modules[__name__]

__cdm_objects = [
    getattr(this,name)
    for name in dir(this)
    if isinstance(getattr(this,name), type)
]
__cdm_object_map = {
    x.name: x
    for x in __cdm_objects
}

def get_cdm_class(key):
    return __cdm_object_map[key]

from .common import DestinationTable
