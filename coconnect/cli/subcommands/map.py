import inspect
import os
import click
import json
import glob
import coconnect
import coconnect.tools as tools

    
@click.group()
def map():
    pass

@click.command(help="Show the OMOP mapping json")
@click.argument("rules")
def show(rules):
    data = tools.load_json(rules)
    print (json.dumps(data,indent=6))

@click.command(help="Display the OMOP mapping json as a DAG")
@click.argument("rules")
def display(rules):
    data = tools.load_json(rules)
    tools.make_dag(data['cdm'],render=True) 

@click.command(help="Generate a python class from the OMOP mapping json")
@click.option("--name",
              required=True,
              help="give the name of the dataset, this will be the name of the .py class file created")
@click.argument("rules")
#@click.option("--",
#              is_flag=True,
#              help="")
def make_class(name,rules):
    data = tools.load_json(rules)
    tools.extract.make_class(data,name)
        
    
@click.command(help="List all the python classes there are available to run")
def list_classes():
    print (json.dumps(tools.get_classes(),indent=6))
        

@click.command(help="Perform OMOP Mapping")
@click.option("--name",
              required=True,
              help="give the name of the dataset, use 'coconnect map list' to see what classes have been registered")
@click.option("--rules",
              required=False,
              help="pass the input json file containing all the mapping rules to be applied")
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
@click.pass_context
def run(ctx,name,rules,inputs,strip_name,drop_csv_from_name,type):

    if not rules is None:
        ctx.invoke(make_class,name=name,rules=rules)
        ctx.invoke(list_classes)

    
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
            x.split("/")[-1][:strip_name].lower()
            if drop_csv_from_name is False
            else
            x.split("/")[-1][:strip_name].lower().replace('.csv','')
        ):x
        for x in inputs
    }

    
    if type == 'csv':
        inputs = tools.load_csv(inputs)
    else:
        raise NotImplementedError("Can only handle inputs that are .csv so far")
        
    available_classes = tools.get_classes()
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
        c = cls(inputs=inputs)
        c.process()
        
    
map.add_command(show,"show")
map.add_command(display,"display")
map.add_command(make_class,"make")
map.add_command(list_classes,"list")
map.add_command(run,"run")
