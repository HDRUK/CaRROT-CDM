import click
import json
import coconnect.tools as tools

# @click.command(help="Perform OMOP Mapping")
# @click.option("--name",
#               required=True,
#               help="the unique name you should provide for the name of the dataset e.g. PANTHER")
# @click.argument("inputs",
#                 nargs=-1)
# @click.option("--map",
#               required=True,
#               help="pass the map .json file")

@click.group()
def map():
    pass

@click.command(help="Show the OMOP mapping json")
@click.argument("input")
def show(input):
    data = json.load(open(input))
    print (json.dumps(data,indent=6))

@click.command(help="Display the OMOP mapping json as a DAG")
@click.argument("input")
def display(input):
    data = json.load(open(input))
    tools.make_dag(data,render=True) 

@click.command(help="Generate a python class from the OMOP mapping json")
@click.argument("name")
@click.argument("input")
def make_class(name,input):
    data = json.load(open(input))
    tools.extract.make_class(name,data)

    
map.add_command(show,"show")
map.add_command(display,"display")
map.add_command(make_class,"make-class")
