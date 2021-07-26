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


@click.command(help="Generate a python class from the OMOP mapping json")
@click.option("--name",
              help="give the name of the dataset, this will be the name of the .py class file created")
@click.option("--register",
              is_flag=True,
              help="also register the python config with the tool")
@click.argument("rules")
@click.pass_context

def make_class(ctx,name,rules,register):
    data = tools.load_json(rules)
    if name == None:
        name = data['metadata']['dataset']

    fname = tools.extract.make_class(data,name)
    if register:
        ctx.invoke(register_class,pyconfig=fname)
    return fname

@click.command(help="Register a python class with the tool")
@click.argument("pyconfig")
def register_class(pyconfig):
    pyconfig = os.path.abspath(pyconfig)
    _,conf_extension = os.path.splitext(pyconfig)
    if conf_extension != '.py':
        raise NotImplementedError(f"You're trying to register the file {pyconfig} which isnt a .py file")
    tools.extract.register_class(pyconfig)

    
@click.command(help="flattern a rules json file")
@click.argument("rules")
def flatten(rules):
    data = tools.load_json(rules)
    objects = data['cdm']
    for destination_table,rule_set in objects.items():
        if len(rule_set) < 2: continue
        #print (rule_set)
        df = pd.DataFrame.from_records(rule_set).T

        #for name in df.index:
        #    print (name,len(df.loc[name]))

        df = df.loc['condition_concept_id'].apply(pd.Series)


        def merge(s):
            if s == 'term_mapping':
                print ('hiya')
                return {k:v for a in s for k,v in a.items()}
        
        print (df.groupby('source_field').agg(merge))
            
        #print (df.iloc[1])
        #print (df.iloc[1][1])
        #print (df)
        #print (df.iloc[0])
        #print (df.iloc[0].name)
        #print (df.iloc[0].apply(pd.Series))
        #print (df.iloc[1].apply(pd.Series)['term_mapping'].apply(pd.Series))
        
        exit(0)

    
@click.command(help="List all the python classes there are available to run")
def list_classes():
    print (json.dumps(tools.get_classes(),indent=6))

@click.command(help="remove a registered class")
@click.pass_context
@click.argument("name")
def remove_class(ctx,name):
    classes = tools.get_classes()
    if name not in classes:
        print (f"{name} is not a registered class")
        ctx.invoke(list_classes)
    else:
        _class = classes[name]
        os.unlink(_class['sympath'])

@click.command(help="Perform OMOP Mapping given an json file")
@click.option("--rules",
              required=True,
              help="input json file containing all the mapping rules to be applied")
@click.option("--type",
              default='csv',
              type=click.Choice(['csv']),
              help="specify the type of inputs, the default is .csv inputs")
@click.option("--use-profiler",
              is_flag=True,
              help="turn on saving statistics for profiling CPU and memory usage")
@click.option("--output-folder",
              default=None,
              help="define the output folder where to dump csv files to")
@click.option("-nc","--number-of-rows-per-chunk",
              default=None,
              type=int,
              help="choose to chunk running the data into nrows")
@click.option("-np","--number-of-rows-to-process",
              default=None,
              type=int,
              help="the total number of rows to process")
@click.argument("inputs",
                #help="give a list of input files to process, and/or an input directory",
                nargs=-1)
@click.pass_context
def run(ctx,rules,inputs,
        output_folder,type,use_profiler,
        number_of_rows_per_chunk,
        number_of_rows_to_process):

    
    if type != 'csv':
        raise NotImplementedError("Can only handle inputs that are .csv so far")
        
    #check if exists
    if any('*' in x for x in inputs):
        data_dir = os.path.dirname(coconnect.__file__)
        data_dir = f'{data_dir}{os.path.sep}data{os.path.sep}'

        new_inputs = []
        for i,x in enumerate(inputs):
            if not os.path.exists(x):
                new_inputs.extend(glob.glob(f"{data_dir}{os.path.sep}{x}"))
            else:
                new_inputs.append(x)
        inputs = new_inputs

    inputs = list(inputs)
    for x in inputs:
        if os.path.isdir(x):
            inputs.remove(x)
            inputs.extend(glob.glob(f'{x}{os.path.sep}*.csv'))
        
    #convert the list into a map between the filename and the full path
    inputs = {
        os.path.basename(x):x
        for x in inputs
    }
    
    if output_folder is None:
        output_folder = f'{os.getcwd()}{os.path.sep}output_data{os.path.sep}'

    inputs = tools.load_csv(inputs,
                            rules=rules,
                            chunksize=number_of_rows_per_chunk,
                            nrows=number_of_rows_to_process)

    config = tools.load_json(rules)
    name = config['metadata']['dataset']
    
    #build an object to store the cdm
    cdm = coconnect.cdm.CommonDataModel(name=name,
                                        inputs=inputs,
                                        output_folder=output_folder,
                                        use_profiler=use_profiler)
    
    #CDM needs to also track the number of rows to chunk
    # - note: should check if this is still needed/used at all
    cdm.set_chunk_size(number_of_rows_per_chunk)
    #loop over the cdm object types defined in the configuration
    #e.g person, measurement etc..
    for destination_table,rules_set in config['cdm'].items():
        #loop over each object instance in the rule set
        #for example, condition_occurrence may have multiple rulesx
        #for multiple condition_ocurrences e.g. Headache, Fever ..
        for i,rules in enumerate(rules_set):
            #make a new object for the cdm object
            #Example:
            # destination_table : person
            # get_cdm_class returns <Person>
            # obj : Person()
            obj = coconnect.cdm.get_cdm_class(destination_table)()
            #set the name of the object
            obj.set_name(f"{destination_table}_{i}")
            
            #call the apply_rules function to setup how to modify the inputs
            #based on the rules
            obj.rules = rules
            #Build a lambda function that will get executed during run time
            #and will be able to apply these rules to the inputs that are loaded
            #(this is useful when chunk)
            obj.define = lambda self : tools.apply_rules(self)
            
            #register this object with the CDM model, so it can be processed
            cdm.add(obj)
            
    cdm.process()


@click.command(help="Perform OMOP Mapping given a python configuration file.")
@click.option("--rules",
              help="input json file containing all the mapping rules to be applied")
@click.option("--pyconf","--conf",
              required=True,
              help="Run with a python configuration file")
@click.option("--type",
              default='csv',
              type=click.Choice(['csv']),
              help="specify the type of inputs, the default is .csv inputs")
@click.option("--use-profiler",
              is_flag=True,
              help="turn on saving statistics for profiling CPU and memory usage")
@click.option("--output-folder",
              default=None,
              help="define the output folder where to dump csv files to")
@click.option("-nc","--number-of-rows-per-chunk",
              default=None,
              type=int,
              help="choose to chunk running the data into nrows")
@click.option("-np","--number-of-rows-to-process",
              default=None,
              type=int,
              help="the total number of rows to process")
@click.argument("inputs",
                #help="give a list of input files to process, and/or an input directory",
                nargs=-1)
@click.pass_context
def run_pyconfig(ctx,rules,pyconf,inputs,
        output_folder,type,use_profiler,
        number_of_rows_per_chunk,
        number_of_rows_to_process):

    
    if type != 'csv':
        raise NotImplementedError("Can only handle inputs that are .csv so far")
        
    #check if exists
    if any('*' in x for x in inputs):
        data_dir = os.path.dirname(coconnect.__file__)
        data_dir = f'{data_dir}{os.path.sep}data{os.path.sep}'

        new_inputs = []
        for i,x in enumerate(inputs):
            if not os.path.exists(x):
                new_inputs.extend(glob.glob(f"{data_dir}{os.path.sep}{x}"))
            else:
                new_inputs.append(x)
        inputs = new_inputs

    inputs = list(inputs)
    for x in inputs:
        if os.path.isdir(x):
            inputs.remove(x)
            inputs.extend(glob.glob(f'{x}{os.path.sep}*.csv'))
        
    #convert the list into a map between the filename and the full path
    inputs = {
        os.path.basename(x):x
        for x in inputs
    }
    
    if output_folder is None:
        output_folder = f'{os.getcwd()}{os.path.sep}output_data{os.path.sep}'

    inputs = tools.load_csv(inputs,
                            rules=rules,
                            chunksize=number_of_rows_per_chunk,
                            nrows=number_of_rows_to_process)

    if pyconf:
        available_classes = tools.get_classes()
        if pyconf not in available_classes:
            ctx.invoke(list_classes)
            raise KeyError(f"cannot find config {pyconf}. Run 'coconnect map py list' to see available classes.")
    
        module = __import__(available_classes[pyconf]['module'],fromlist=[pyconf])
        defined_classes = [
            m[0]
            for m in inspect.getmembers(module, inspect.isclass)
            if m[1].__module__ == module.__name__
        ]
        #should only be running one class anyway
        defined_class = defined_classes[0]
        cls = getattr(module,defined_class)
        #build a class object
        cdm = cls(inputs=inputs,
                  output_folder=output_folder,
                  use_profiler=use_profiler)
        cdm.set_chunk_size(number_of_rows_per_chunk)
        #run it
        cdm.process()
                
    elif rules:
        config = tools.load_json(rules)
        name = config['metadata']['dataset']

        #build an object to store the cdm
        cdm = coconnect.cdm.CommonDataModel(name=name,
                                            inputs=inputs,
                                            output_folder=output_folder,
                                            use_profiler=use_profiler)
        
        #CDM needs to also track the number of rows to chunk
        # - note: should check if this is still needed/used at all
        cdm.set_chunk_size(number_of_rows_per_chunk)
        #loop over the cdm object types defined in the configuration
        #e.g person, measurement etc..
        for destination_table,rules_set in config['cdm'].items():
            #loop over each object instance in the rule set
            #for example, condition_occurrence may have multiple rulesx
            #for multiple condition_ocurrences e.g. Headache, Fever ..
            for i,rules in enumerate(rules_set):
                #make a new object for the cdm object
                #Example:
                # destination_table : person
                # get_cdm_class returns <Person>
                # obj : Person()
                obj = coconnect.cdm.get_cdm_class(destination_table)()
                #set the name of the object
                obj.set_name(f"{destination_table}_{i}")
                
                #call the apply_rules function to setup how to modify the inputs
                #based on the rules
                obj.rules = rules
                #Build a lambda function that will get executed during run time
                #and will be able to apply these rules to the inputs that are loaded
                #(this is useful when chunk)
                obj.define = lambda self : tools.apply_rules(self)
                
                #register this object with the CDM model, so it can be processed
                cdm.add(obj)
    
        cdm.process()
    else:
        raise NotImplementedError("You need to run the CLI tool with either a json or python configuration file")


@click.group(help="Commands for using python configurations to run the ETL transformation.")
def py():
    pass

py.add_command(make_class,"make")
py.add_command(register_class,"register")
py.add_command(list_classes,"list")
py.add_command(remove_class,"remove")
py.add_command(run_pyconfig,"run")
map.add_command(py,"py")

map.add_command(run,"run")
map.add_command(diff,"diff")
#map.add_command(flatten,"flatten")
