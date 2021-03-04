from .models import Person, ConditionOccurrence

def define_person(defs):
    p = Person()
    p.define = defs
    return p

def define_condition_occurrence(defs):
    c = ConditionOccurrence()
    c.define = defs
    return c
