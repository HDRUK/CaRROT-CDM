import os
import inspect
import click
import json
import glob
import coconnect
import coconnect.tools as tools
from coconnect.cdm import load_csv

def get_file(f_in):
    try:
        data = json.load(open(f_in))
    except FileNotFoundError as err:
        try:
            data_dir = os.path.dirname(coconnect.__file__)
            data_dir = f'{data_dir}/data/'
            data =  json.load(open(f'{data_dir}{f_in}'))
        except FileNotFoundError:
            raise FileNotFoundError(err)

    return data

    
@click.group()
def map():
    pass

@click.command(help="Show the OMOP mapping json")
@click.argument("rules")
def show(rules):
    data = get_file(rules)
    print (json.dumps(data,indent=6))

@click.command(help="Display the OMOP mapping json as a DAG")
@click.argument("rules")
def display(rules):
    data = get_file(rules)
    tools.make_dag(data,render=True) 

@click.command(help="Generate a python class from the OMOP mapping json")
@click.option("--name",
              required=True,
              help="give the name of the dataset, this will be the name of the .py class file created")
@click.argument("rules")
#@click.option("--",
#              is_flag=True,
#              help="")
def make_class(name,rules):
    data = get_file(rules)
    tools.extract.make_class(data,name)

    
def get_classes():
    import time
    from coconnect.cdm import classes
    _dir = os.path.dirname(classes.__file__)
    files = [x for x in os.listdir(_dir) if x.endswith(".py") and not x.startswith('__')]
    retval = {}
    for fname in files:
        mname = fname.split(".")[0]
        mname = '.'.join([classes.__name__, mname])
        module = __import__(mname,fromlist=[fname])
        defined_classes = {
            m[0]: {
                'module':m[1].__module__,
                'path': os.path.join(_dir,fname),
                'last-modified': time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(os.path.getmtime(os.path.join(_dir,fname))))
            }
            for m in inspect.getmembers(module, inspect.isclass)
            if m[1].__module__ == module.__name__
        }
        retval.update(defined_classes)
    return retval
        
    
@click.command(help="List all the python classes there are available to run")
def list_classes():
    print (json.dumps(get_classes(),indent=6))
        

@click.command(help="Perform OMOP Mapping")
@click.option("--name",
              required=True,
              help="give the name of the dataset, use 'coconnect map list' to see what classes have been registered")
@click.option("--type",
              default='csv',
              type=click.Choice(['csv']),
              help="specify the type of inputs, the default is .csv inputs")
@click.option("--strip-name",
              default=None,
              type=int,
              help="limit the number of chars in the key name for inputs {key:file}\
              , useful with WhiteRabbit synthetic data, which is often limited to 31 characters")
@click.option("--strip-name",
              default=None,
              type=int,
              help="handy tool to strip the name of the input to match with whiterabbit")
@click.option("--drop-csv-from-name",
              is_flag=True,
              help="handy tool to drop .csv. from the key name, may be needed with whiterabbit")
@click.argument("inputs",
                nargs=-1)
def run(name,inputs,strip_name,drop_csv_from_name,type):

    #check if exists
    if any('*' in x for x in inputs):
        data_dir = os.path.dirname(coconnect.__file__)
        data_dir = f'{data_dir}/data/'

        new_inputs = []
        for i,x in enumerate(inputs):
            if not os.path.exists(x):
                new_inputs.extend(glob.glob(f"{data_dir}/{x}"))
            else:
                new_inputs.append(x)
        inputs = new_inputs


            
    inputs = {
        (
            x.split("/")[-1][:strip_name]
            if drop_csv_from_name is False
            else
            x.split("/")[-1][:strip_name].replace('.csv','')
        ):x
        for x in inputs
    }

    
    if type == 'csv':
        inputs = load_csv(inputs)
    else:
        raise NotImplementedError("Can only handle inputs that are .csv so far")
        
    available_classes = get_classes()
    if name not in available_classes:
        print (available_classes)
        raise KeyError(f"cannot find config for {name}")

    module = __import__(available_classes[name]['module'],fromlist=[name])
    defined_classes = [
        m[0]
        for m in inspect.getmembers(module, inspect.isclass)
        if m[1].__module__ == module.__name__
    ]

    for defined_class in defined_classes:
        cls = getattr(module,defined_class)
        cls.inputs = inputs
        c = cls()
        c.process()
        
    
map.add_command(show,"show")
map.add_command(display,"display")
map.add_command(make_class,"make")
map.add_command(list_classes,"list")
map.add_command(run,"run")
