import os
import inspect
import click
import json
import coconnect.tools as tools
from coconnect.cdm import load_csv
    

@click.group()
def map():
    pass

@click.command(help="Show the OMOP mapping json")
@click.argument("rules")
def show(rules):
    data = json.load(open(rules))
    print (json.dumps(data,indent=6))

@click.command(help="Display the OMOP mapping json as a DAG")
@click.argument("rules")
def display(rules):
    data = json.load(open(rules))
    tools.make_dag(data,render=True) 

@click.command(help="Generate a python class from the OMOP mapping json")
@click.argument("name")
@click.argument("rules")
#@click.option("--",
#              is_flag=True,
#              help="")
def make_class(name,rules):
    data = json.load(open(rules))
    tools.extract.make_class(name,data)

    
def get_classes():
    from coconnect.cdm import classes
    _dir = os.path.dirname(classes.__file__)
    files = [x for x in os.listdir(_dir) if x.endswith(".py") and not x.startswith('__')]
    retval = {}
    for fname in files:
        mname = fname.split(".")[0]
        mname = '.'.join([classes.__name__, mname])
        module = __import__(mname,fromlist=[fname])
        defined_classes = {
            m[0]:m[1].__module__
            for m in inspect.getmembers(module, inspect.isclass)
            if m[1].__module__ == module.__name__
        }
        retval.update(defined_classes)
    return retval
        
    
@click.command(help="List all the python classes there are available to run")
def list_classes():
    print (json.dumps(get_classes(),indent=6))
        

@click.command(help="Perform OMOP Mapping")
@click.argument("dataset")
@click.argument("inputs",
                nargs=-1)
def run(dataset,inputs):

    inputs = load_csv(
        {
            x.split("/")[-1]:x
            for x in inputs
        })

    available_classes = get_classes()
    if dataset not in available_classes:
        print (available_classes)
        raise KeyError(f"cannot find config for {dataset}")

    module = __import__(available_classes[dataset],fromlist=[dataset])
    defined_classes = [
        m[0]
        for m in inspect.getmembers(module, inspect.isclass)
        if m[1].__module__ == module.__name__
    ]

    for defined_class in defined_classes:
        cls = getattr(module,defined_class)
        cls(inputs=inputs)
    
map.add_command(show,"show")
map.add_command(display,"display")
map.add_command(make_class,"make")
map.add_command(list_classes,"list")
map.add_command(run,"run")
