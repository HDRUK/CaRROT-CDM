from .objects import (
    Person,
    ConditionOccurrence,
    VisitOccurrence,
    Measurement,
    Observation,
    DrugExposure
)

class LoadTableError(Exception):
    pass


def load_file(fname):
    def func(self):
        for colname in self.inputs[fname]:
            self[colname].series = self.inputs[fname][colname]
    func.__name__ = fname
    return func


# def from_table(obj,table):
#     df = obj.inputs[table]
#     for colname in df.columns:
#         obj[colname].series = df[colname]
#     return obj


# def define_person(input=None):
#     print ("define person")
#     def decorator(defs):
#         print ("in decorator")
#         def wrapper(obj):
#             if input is not None:
#                 obj = from_table(obj,input)
#             defs(obj)

#         p = Person()
#         p.define = wrapper 
#         p.set_name(defs.__name__)
#         return p
#     print (decorator)
#     return decorator

def load_table(table,filter=None):
    def decorator(defs):
        def wrapper(obj):
            try:
                df = obj.inputs[table]
            except Exception as err:
                raise LoadTableError(f"Using the decorator {load_table} gave the error:\n"+str(err))

            
            if filter is not None:
                for key,value in filter.items():
                    df = df[df[key]==value]

                print (df)
            for colname in df.columns:
                obj[colname].series = df[colname]

            defs(obj)
                
        wrapper.__name__ = defs.__name__
        return wrapper
    return decorator

def define_person(defs):
    p = Person()
    p.define = defs
    p.set_name(defs.__name__)
    return p

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
