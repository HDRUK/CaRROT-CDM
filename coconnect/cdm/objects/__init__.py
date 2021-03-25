from .person import Person
from .condition_occurrence import ConditionOccurrence
from .visit_occurrence import VisitOccurrence
from .measurement import Measurement

class Init(object):
    """
    basic class just to handle any initialise functions
    """
    def define(self,_):
        """
        define function, expected to be overloaded by the user defining an init object
        """
        return self
