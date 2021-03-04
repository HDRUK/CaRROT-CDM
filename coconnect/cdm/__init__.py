import pandas as pd
from types import SimpleNamespace 
from .models import CommonDataModel, Person, ConditionOccurrence

def person(func):
    p = Person()
    p.define = func
    return p

def condition_occurrence(func):
    c = ConditionOccurrence()
    c.define = func
    return c


def load_csv(_map,nrows=None,load_path=""):
    for key,fname in _map.items():
        df = pd.read_csv(load_path+fname,nrows=nrows)
        for col in df.columns:
            df[col].fname = fname
        _map[key] = df 
    return SimpleNamespace(**_map)
