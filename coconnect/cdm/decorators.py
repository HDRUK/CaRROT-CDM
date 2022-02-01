from .objects import (
    Person,
    ConditionOccurrence,
    VisitOccurrence,
    Measurement,
    Observation,
    DrugExposure
)


def load_file(_input):
    def func(self):
        for colname in _input:
            self[colname].series = _input[colname]
            #print (_input)
    #func.__name__ = fname
    return func


def from_table(obj,table):
    df = obj.inputs[table]
    for colname in df.columns:
        obj[colname].series = df[colname]
    return obj


def define_person(defs):
    c = Person()
    c.define = defs
    c.set_name(defs.__name__)
    return c

def define_condition_occurrence(defs):
    c = ConditionOccurrence()
    c.define = defs
    c.set_name(defs.__name__)
    return c

def define_visit_occurrence(defs):
    c = VisitOccurrence()
    c.define = defs
    c.set_name(defs.__name__)
    return c

def define_measurement(defs):
    c = Measurement()
    c.define = defs
    c.set_name(defs.__name__)
    return c

def define_observation(defs):
    c = Observation()
    c.define = defs
    c.set_name(defs.__name__)
    return c

def define_drug_exposure(defs):
    c = DrugExposure()
    c.define = defs
    c.set_name(defs.__name__)
    return c
