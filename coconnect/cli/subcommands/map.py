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
def make_class(name,rules):
    data = json.load(open(rules))
    tools.extract.make_class(name,data)

@click.command(help="Perform OMOP Mapping")
@click.argument("inputs",
                nargs=-1)
@click.option("--file",
              required=True,
              help="pass the mapping .py file")
def run(inputs,file):

    inputs = load_csv(
        {
            x.split("/")[-1]:x
            for x in inputs
        })
    
    fname = file.split(".")[0]
    module = __import__(fname)
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
map.add_command(run,"run")
