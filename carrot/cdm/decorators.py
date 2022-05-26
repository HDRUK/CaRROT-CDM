
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

       
class qsub(object):

    def __init__(self,jobscript,**kwargs):
        self.default_kwargs = kwargs
        self.jobscript = jobscript

    def set_jobscript(self,jobscript):
        self.jobscript = jobscript
    
    def __call__(self, analysis, *args, **kwargs):
        return self._qsub(analysis, *args, **kwargs)

    def make_jobscript(self,**kwargs):
        return self.jobscript.format(**kwargs)
    
    @classmethod
    def eddie(cls,**kwargs):
        default_kwargs={'runtime':"00:30:00",'memory':"1G",'anaconda':"5.3.1"}
        default_kwargs.update(kwargs)
        jobscript = r'''
#!/bin/sh
# Grid Engine options (lines prefixed with #$)
#$ -N {job_name}             
#$ -cwd                  
#$ -l h_rt={runtime}
#$ -l h_vmem={memory}

# Initialise the environment modules
. /etc/profile.d/modules.sh

# Load Python
module load anaconda/{anaconda}

# Load the virtual environment
source /exports/applications/apps/SL7/anaconda/5.3.1/etc/profile.d/conda.sh
conda activate carrot-env

{command}
'''
        obj = cls(jobscript=jobscript,**default_kwargs)
        
        return obj

    def run(self,commands,jobname="qsub_job"):
        kwargs = self.default_kwargs
        kwargs.update({'job_name':jobname,'command':" ".join(commands)})
        jobscript = self.make_jobscript(**kwargs)
        fname = f"{jobname}.sh"
        with open(fname,"w") as f:
            f.write(jobscript)
        output = subprocess.check_output(['qsub','-N',jobname,fname]).decode()
        return output

    def _qsub(self,analysis,*args,**kwargs):        
        def wrapper(model,*args,**kwargs):
            commands = copy.copy(sys.argv)
            if "--analysis" in commands:
                return analysis(model,*args,**kwargs)
            else:
                commands.extend(["--analysis",analysis.__name__])
                name = "analysis"
                _id = format(id(analysis),'X')
                return self.run(commands=commands,jobname=f"{name}_{analysis.__name__}_{_id}")
        return wrapper
    #return _qsub

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

def define_table(cls):
    def decorator(defs):
        obj = cls()
        obj.define = defs
        obj.set_name(defs.__name__)
        return obj
    return decorator

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

