import pandas as pd
import inspect
import os
import click
import json
import glob
import coconnect
import coconnect.tools as tools

    
@click.group(help="Commands for mapping data to the OMOP CommonDataModel (CDM).")
def map():
    pass

@click.command(help="Detect differences in either inputs or output csv files")
@click.argument("file1")
@click.argument("file2")
def diff(file1,file2):
    df1 = pd.read_csv(file1)
    df2 = pd.read_csv(file2)

    exact_match = df1.equals(df2)
    if exact_match:
        return

    df = pd.concat([df1,df2]).drop_duplicates(keep=False)
    if len(df) > 0:
        print (" ======== Differing Rows ========== ")
        print (df)
        m = df1.merge(df2, on=df.columns[0], how='outer', suffixes=['', '_'], indicator=True)[['_merge']]
        m = m[~m['_merge'].str.contains('both')]
        file1 = file1.split('/')[-1]
        file2 = file2.split('/')[-1]
        
        m['_merge'] = m['_merge'].map({'left_only':file1,'right_only':file2})
        m = m.rename(columns={'_merge':'Only Contained Within'})
        m.index.name = 'Row Number'
        print (m.reset_index().to_dict(orient='records'))

    else:
        print (" ======= Rows are likely in a different order ====== ")
        for i in range(len(df1)):
            if not (df1.iloc[i] == df2.iloc[i]).any():
                print ('Row',i,'is in a different location')

@click.command(help="Show the OMOP mapping json")
@click.argument("rules")
def show(rules):
    data = tools.load_json(rules)
    print (json.dumps(data,indent=6))


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
@click.option("--output-folder",
              default=None,
              help="define the output folder where to dump csv files to")
@click.argument("inputs",
                nargs=-1)
@click.pass_context
def run(ctx,
        name,rules,inputs,output_folder,
        strip_name,drop_csv_from_name,type):

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

    source_map = None
    if rules is not None:
        config = tools.load_json(rules)['cdm']
        #extract a tuple of source tables and source fields
        sources = [
            (x['source_table'],x['source_field'])
            for cdm_obj_set in config.values()
            for cdm_obj in cdm_obj_set
            for x in cdm_obj.values()
        ]

        source_map = {}
        for (table,field) in sources:
            if table not in source_map:
                source_map[table] = []
            source_map[table].append(field)

        source_map = {
            k:list(set(v))
            for k,v in source_map.items()
        }


    inputs = {
        (
            x.split("/")[-1][:strip_name].lower()
            if drop_csv_from_name is False
            else
            x.split("/")[-1][:strip_name].lower().replace('.csv','')
        ):x
        for x in inputs
    }

    fields = None
    #reduce the mapping of inputs, if we dont need them all
    if source_map is not None:
        inputs = {
            k: {
                'file':v,
                'fields':source_map[k]
            }
            for k,v in inputs.items()
            if k in source_map
        }
    if type == 'csv':
        inputs = tools.load_csv(inputs)
    else:
        raise NotImplementedError("Can only handle inputs that are .csv so far")
        
    available_classes = tools.get_classes()
    if name not in available_classes:
        raise KeyError(f"cannot find config for {name}")

    module = __import__(available_classes[name]['module'],fromlist=[name])
    defined_classes = [
        m[0]
        for m in inspect.getmembers(module, inspect.isclass)
        if m[1].__module__ == module.__name__
    ]

    
    if output_folder is None:
        output_folder = os.getcwd()+'/output_data/'

    
    for defined_class in defined_classes:
        cls = getattr(module,defined_class)
        c = cls(inputs=inputs,
                output_folder=output_folder)
        c.process()
        
    
map.add_command(show,"show")
map.add_command(make_class,"make")
map.add_command(list_classes,"list")
map.add_command(run,"run")
map.add_command(diff,"diff")
