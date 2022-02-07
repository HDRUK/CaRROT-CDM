
from .objects import (
    Person,
    ConditionOccurrence,
    VisitOccurrence,
    Measurement,
    Observation,
    DrugExposure
)
import copy
import sys
import time

import subprocess

class analysis(object):
    def __init__(self, method):
        self._method = method
        self._name = method.__name__
    def __call__(self, obj, *args, **kwargs):
        return self._method(obj, *args, **kwargs)

def qsub(**kwargs):
    def _qsub(analysis):
        def start(commands):
            print (kwargs)
            retval = subprocess.check_output(['echo']+commands).decode('utf-8')
            return retval
        
        def wrapper(self,*args,**kwargs):
            commands = copy.copy(sys.argv)
            if "--analysis" in commands:
                return analysis(self,*args,**kwargs)
            else:
                commands.extend(["--analysis",analysis.__name__])
                return start(commands)
        return wrapper
    return _qsub

def load_file(_input):
    def func(self):
        for colname in _input:
            self[colname].series = _input[colname]
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

