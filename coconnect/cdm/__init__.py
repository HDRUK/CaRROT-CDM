import pandas as pd
import copy
import json
from types import SimpleNamespace 
from .model import CommonDataModel

from .objects import Person, ConditionOccurrence, VisitOccurrence, Measurement
from .decorators import define_person, define_condition_occurrence, define_visit_occurrence, define_measurement


def load_csv(_map,nrows=None,load_path=""):
    for key,fname in _map.items():
        df = pd.read_csv(load_path+fname,nrows=nrows)
        for col in df.columns:
            df[col].fname = fname
        _map[key] = df 
    return _map

def to_name_space(_map):
    return SimpleNamespace(**_map)

