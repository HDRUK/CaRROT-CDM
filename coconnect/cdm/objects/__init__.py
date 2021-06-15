from .person import Person
from .condition_occurrence import ConditionOccurrence
from .visit_occurrence import VisitOccurrence
from .measurement import Measurement
from .observation import Observation

import sys
this = sys.modules[__name__]

_cdm_objects = [
    getattr(this,name)
    for name in dir(this)
    if isinstance(getattr(this,name), type)
]

_cdm_object_map = {
    x.name: x
    for x in _cdm_objects
}
