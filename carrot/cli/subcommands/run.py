import pandas as pd
import inspect
import os
import sys
import click
import json
import yaml
import glob
import copy
import subprocess
import carrot
import carrot.tools as tools

@click.group(help="Commands for mapping data to the OMOP CommonDataModel (CDM).")
def run():
    pass


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

@click.command(help="Execute the running of the test dataset")
@click.pass_context
def test(ctx):
    _dir = os.path.dirname(os.path.abspath(carrot.__file__))
    _dir = f"{_dir}{os.path.sep}data{os.path.sep}test"
    inputs = glob.glob(f"{_dir}{os.path.sep}inputs{os.path.sep}*.csv")
    rules = f"{_dir}{os.path.sep}rules{os.path.sep}rules_14June2021.json"
    output_folder = 'tests'
    ctx.invoke(run,inputs=inputs,rules=rules,output_folder=output_folder)
    for fname in glob.glob(f"{output_folder}{os.path.sep}*.tsv"):
        tools.diff_csv(fname,fname)


@click.command(help="apply operations (transforms) to a dataset")
@click.option("--config","-c",required=True,type=str)
@click.option("--number-of-rows-per-chunk","--nc",default=1e5,type=int)
@click.option("--output-folder","-o",required=True,type=str)
@click.argument("inputs",nargs=-1,required=True)
def transform(inputs,config,number_of_rows_per_chunk,output_folder):

    logger = tools.logger.Logger("transform")
    
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
    
    if os.path.isfile(config):
        config = json.load(open(config))
    else:
        config = json.loads(config)

    logger.info(json.dumps(config,indent=6))
        
    inputs = {
        os.path.basename(x):x
        for x in inputs
    }
    input_data = tools.load_csv(inputs,chunksize=number_of_rows_per_chunk)

    operation_tools = carrot.cdm.OperationTools()

    header=True
    mode='w'

    i = 0 
    while True:
        if i > 0:
            mode = 'a'
            header=False
        i+=1
        for fname,rules in config.items():
            logger.info(f"Working on {fname}") 
            df = input_data[fname]
            for colname,operations in rules.items():
                series = df[colname]
                for operation in operations:
                    logger.info(f".. transforming '{colname}' with operation '{operation}'")
                    fn_operation = operation_tools[operation]
                    series = fn_operation(series)
                df[colname] = series
            fout = f"{output_folder}/{fname}"
            df.to_csv(fout,header=header,mode=mode,index=False)

        try:
            input_data.next()
        except StopIteration:
            break

@click.command()
@click.option("--number-of-rows-per-chunk","--nc",default=1e5,type=int)
@click.option("--output-folder","-o",required=True,type=str)
@click.argument("inputs",nargs=-1,required=True)
def format(inputs,number_of_rows_per_chunk,output_folder):
    """
    Format a CDM model by passing all CDM objects and applying formatting.
    """
    types = list(set([
        os.path.splitext(fname)[1]
        for fname in inputs
    ]))
    if len(types) > 1:
        raise Exception(f"Running with mixed input files '{types}'. Only input tsv or csv files.")
    types = types[0]
    
    inputs = {
        os.path.basename(x):x
        for x in inputs
    }
    if types == '.csv':
        inputs = tools.load_csv(inputs,chunksize=number_of_rows_per_chunk)
    else:
        inputs = tools.load_tsv(inputs,chunksize=number_of_rows_per_chunk)

    cdm = carrot.cdm.CommonDataModel.from_existing(inputs=inputs,
                                                      output_folder=output_folder,
                                                      format_level=1)
    #cdm.save_files = False
    cdm.process()
    cdm.end()

@click.command()
@click.option("--output-folder","-o",required=True,type=str,help='specify an output folder of where to put the merged tables')
@click.argument("inputs",nargs=-1,required=True)
def merge(inputs,output_folder):
    """
    Build a CDM model from subfiles
    """
    types = list(set([
        os.path.splitext(fname)[1]
        for fname in inputs
    ]))
    if len(types) > 1:
        raise Exception(f"Running with mixed input files '{types}'. Only input tsv or csv files.")
    types = types[0]
    
    inputs = {
        os.path.basename(x):x
        for x in inputs
    }
    if types == '.csv':
        inputs = tools.load_csv(inputs)#,chunksize=number_of_rows_per_chunk)
    else:
        inputs = tools.load_tsv(inputs)#,chunksize=number_of_rows_per_chunk)

    outputs = carrot.tools.create_csv_store(output_folder=output_folder,sep='\t',write_mode='w',write_separate=False)
        
    cdm = carrot.cdm.CommonDataModel.from_existing(inputs=inputs,
                                                      do_mask_person_id=False,
                                                      drop_duplicates=True,
                                                      format_level=0,
                                                      outputs=outputs)
    cdm.process()


@click.command(help="loading")
@click.option("--config","-c",required=True,type=str)
@click.argument("inputs",nargs=-1,required=True)
def load(inputs,config):
    inputs = {os.path.basename(x).split('.')[0]:x for x in inputs}
    config = json.loads(config)
    if 'bclink' in config:
        outputs = carrot.io.BCLinkDataCollection(config['bclink'])
    else:
        raise NotImplementedError(f"Not setup to implement configuration for load {config}")

    for destination_table,fname in inputs.items():
        outputs.load(fname,destination_table)

    outputs.finalise()
    
@click.command()
@click.option("--input",
              required=True,
              help='input csv file')
@click.option("--column",
              required=True,
              help="which column of the input data to modify")
@click.option("--operation",
              required=True,
              help="which operation to apply to the column")
def format_input_data(column,operation,input):
    """
    Useful formatting command for applying an operation on some data before being passed into the ETL-Tool

    """
    optools = carrot.cdm.OperationTools()
    allowed_operations = optools.keys()
    if operation not in allowed_operations:
        raise Exception(f"Operation '{operation}' is not a known operation. Choose from {allowed_operations}")
    df_input = pd.read_csv(input)
    df_input[column] = optools[operation](df_input[column])
    n = 5 if len(df_input) > 5 else len(df_input)
    print (df_input[column].sample(n))
    df_input.to_csv(input)


def _condor(name,commands,jobscript):
    #hold_jid {name} - wait for previous job
    def wrapper(args):
        script = jobscript + '\n' + ' '.join(commands)
        fname = f"{name}.sh"
        with open(fname,"w") as f:
            f.write(script)
        output = subprocess.check_output(['qsub','-N',name,fname]).decode()
        return output
    return wrapper

from importlib import import_module

@click.command()
@click.option("analysis_name","--analysis",default=None,type=str)
@click.argument("analysis_file")
@click.pass_context
def analysis(ctx,analysis_file,analysis_name):
    """
    Use this command to run analyses on input data (in the CDM format) given a configuration yaml file
    """
    _dir = os.path.dirname(analysis_file)
    fname = os.path.splitext(os.path.basename(analysis_file))[0]
    sys.path.append(_dir)
    module = import_module(fname)
    clsmembers = inspect.getmembers(module, inspect.isclass)
    name,cls = clsmembers[0]
    obj = cls()
    if analysis_name:
        ana = obj.get_analysis(analysis_name)
        res = obj.run_analysis(ana)
        print (res)
    else:
        #print (obj.get_analyses())
        #exit(0)
        obj.run_analyses()


@click.command()
@click.option("--max-workers",default=None,type=int)
@click.option("--batch",default=None,type=click.Choice(['condor']))
@click.option("analysis_names","--analysis-name",default=None,multiple=True,type=str)
@click.argument("config")
@click.pass_context
def ___analysis(ctx,config,analysis_names,max_workers,batch):
    """
    Use this command to run analyses on input data (in the CDM format) given a configuration yaml file
    """    
    from importlib import import_module
    fname = config
    stream = open(config) 
    config = yaml.safe_load(stream)
    
    inputs = config['cdm']
    inputs = carrot.tools.load_tsv(config['cdm'],
                                      dtype=None)

    cdm = carrot.cdm.CommonDataModel.load(inputs=inputs)

    analyses = config['analyses']

    jobscript = config['condor']['jobscript']
    

    if analysis_names:
        temp = copy.copy(analyses)
        for name in temp:
            if name not in analysis_names:
                analyses.pop(name)

    for name,_def in analyses.items():
        analysis = _def['analysis']
        cohort = _def['filter']
        if batch:
            if batch == 'condor':
                commands = copy.copy(sys.argv)
                commands.remove('--batch')
                commands.remove('condor')
                commands.extend(['--analysis-name',name])
                
                f = _condor(name=name,commands=commands,jobscript=jobscript)
            else:
                raise NotImplementedError(f"{batch} mode for --batch not a thing")
        else:
            func = import_module(analysis)
            f = func.create_analysis(cohort)
        cdm.add_analysis(f)
    
    results = cdm.run_analyses(max_workers=max_workers)

    
        
@click.command()
@click.option("--rules",
              required=True,
              help="input json file containing all the mapping rules to be applied")
@click.option("--indexing-conf",
              default=None,
              help="configuration file to specify how to start the indexing")
@click.option("--csv-separator",
              default='\t',
              type=click.Choice([';',':','\t',',',' ',]),
              help="choose a separator to use when dumping output csv files")
@click.option("--use-profiler",
              is_flag=True,
              help="turn on saving statistics for profiling CPU and memory usage")
@click.option("format_level","--format-level",
              default='1',
              type=click.Choice(['0','1','2']),
              help="Choose the level of formatting to apply on the output data. 0 - no formatting. 1 - automatic formatting. 2 (default) - check formatting (will crash if input data is not already formatted).")
@click.option("--output-folder",
              default=None,
              help="define the output folder where to dump csv files to")
@click.option("--write-mode",
              default='w',
              type=click.Choice(['w','a']),
              help="force the write-mode on existing files")
@click.option("--split-outputs",
              is_flag=True,
              help="force the output files to be split into separate files")
@click.option("--allow-missing-data",
              is_flag=True,
              help="don't crash if there is data tables in rules file that hasnt been loaded")
@click.option("output_database","--database",
              default=None,
              help="define the output database where to insert data into")
@click.option("-nc","--number-of-rows-per-chunk",
              default=-1,
              help="Choose the number of rows (INTEGER) of input data to load (chunksize). The option 'auto' will work out the ideal chunksize. Inputing a value <=0 will turn off data chunking (default behaviour).")
@click.option("-np","--number-of-rows-to-process",
              default=None,
              type=int,
              help="the total number of rows to process")
@click.option("--person-id-map",
              default=None,
              help="pass the location of a file containing existing masked person_ids")
@click.option("--db",
              default=None,
              help="instead, pass a connection string to a db")
@click.option("--merge-output",
              is_flag=True,
              help="merge the output into one file")
@click.option("no_mask_person_id","--parse-original-person-id",
              is_flag=True,
              help="turn off automatic conversion (creation) of person_id to (as) Integer")
@click.option("dont_automatically_fill_missing_columns","--no-fill-missing-columns",
              is_flag=True,
              help="Turn off automatically filling missing CDM columns")
@click.option("log_file","--log-file",
              default = 'none',
              help="specify a path for a log file")
@click.option("--max-rules",
              default = None,
              type=int,
              help="maximum number of rules to process")
@click.option("objects","--object",
              default = None,
              multiple=True,
              type=str,
              help="give a list of objects by name to process")
@click.option("tables","--table",
              default = None,
              multiple=True,
              type=str,
              help="give a list of tables by name to process")
@click.argument("inputs",
                required=False,
                nargs=-1)
@click.pass_context
def map(ctx,rules,inputs,format_level,
        output_folder,output_database,
        csv_separator,use_profiler,log_file,
        no_mask_person_id,indexing_conf,
        person_id_map,max_rules,merge_output,
        objects,tables,db,write_mode,split_outputs,
        dont_automatically_fill_missing_columns,
        number_of_rows_per_chunk,allow_missing_data,
        number_of_rows_to_process):
    """
    Perform OMOP Mapping given an json file and a series of input files

    INPUTS should be a space separated list of individual input files or directories (which contain .csv files)
    """

    if output_folder is None:
        output_folder = f'{os.getcwd()}{os.path.sep}output_data{os.path.sep}'

    #if log_file == 'auto' and carrot.params['log_file'] is None:
    if log_file == 'auto':
        log_file = f"{output_folder}{os.path.sep}logs{os.path.sep}carrot.log"
        carrot.params['log_file'] = log_file
    elif log_file == 'none':
        pass
    else:
        carrot.params['log_file'] = log_file
        
    #load the json loads
    if type(rules) == dict:
        config = rules
    else:
        config = tools.load_json(rules)

    if tables:
        tables = list(set(tables))
        config = carrot.tools.filter_rules_by_destination_tables(config,tables)

    if objects:
        objects = list(set(objects))
        config = carrot.tools.filter_rules_by_object_names(config,objects)
        
    if max_rules:
        i = 0
        n = max_rules
        new = {}
        for destination_table,rule_set in config['cdm'].items():
            if destination_table == 'person':
                new[destination_table] = rule_set
            else:
                for name,_rules in rule_set.items():
                    if i>=n:
                        break
                    if destination_table not in new:
                        new[destination_table] = {}
                    new[destination_table][name] = _rules
                    i+=1
            
        config['cdm'] = new

    name = config['metadata']['dataset']

    if indexing_conf is not None:
        if isinstance(indexing_conf,dict):
            pass
        elif indexing_conf.endswith(".json") and os.path.exists(indexing_conf):
            indexing_conf = tools.load_json(indexing_conf)
        elif indexing_conf.endswith(".csv") and os.path.exists(indexing_conf):
            try:
                indexing_conf = pd.read_csv(indexing_conf,header=None,index_col=0)[1].to_dict()
            except pd.errors.EmptyDataError:
                indexing_conf = None
                pass
                
    
    #automatically calculate the ideal chunksize
    if number_of_rows_per_chunk == 'auto':
        #get the fields that are going to be used/loaded
        used_fields = tools.get_mapped_fields_from_rules(config)
        #calculate the number of fields that are to be used per dataset
        n_used_fields = [ len(sublist) for sublist in used_fields.values() ]
        #find what's the largest number of fields loaded by any dataset
        max_n_used_fields = max(n_used_fields)
        #get the number of files used
        n_files = len(n_used_fields)
        
        # If there is one dataset and one column being used, the max loaded to memory
        #   is 2million rows (this is fairly arbitrary)
        #   it is an approximation assuming the data in the values is relatively small
        #   this should keep the memory usage down
        # When there is more fields and more files loaded, reduce the of rows per chunk
        max_n_rows = 2e6
        number_of_rows_per_chunk = int(max_n_rows/(max_n_used_fields*n_files))
    else:
        try:
            number_of_rows_per_chunk = int(number_of_rows_per_chunk)
        except ValueError:
            raise ValueError(f"number_of_rows_per_chunk must be an Integer or 'auto', you inputted '{number_of_rows_per_chunk}'")
        
        #turn off chunking if 0 or negative chunksizes are given
        if number_of_rows_per_chunk <= 0 :
            number_of_rows_per_chunk = None
    
    #check if exists
    if any('*' in x for x in inputs):
        data_dir = os.path.dirname(carrot.__file__)
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

    if db:
        inputs = tools.load_sql(connection_string=db,chunksize=number_of_rows_per_chunk,nrows=number_of_rows_to_process)
    else:
        if allow_missing_data:
            config = carrot.tools.remove_missing_sources_from_rules(config,inputs)

        inputs = tools.load_csv(inputs,
                                rules=config,
                                chunksize=number_of_rows_per_chunk,
                                nrows=number_of_rows_to_process)

    #do something with
    #person_id_map
        
    if isinstance(output_database,dict):
        if 'bclink' in output_database:
            outputs = carrot.tools.create_bclink_store(bclink_settings=output_database['bclink'],
                                                          output_folder=output_database['cache'],
                                                          sep=csv_separator,
                                                          write_separate=split_outputs,
                                                          write_mode=write_mode)
        else:
            raise NotImplementedError(f"dont know how to configure outputs... {output_database}")   
    elif output_database == None:
        outputs = carrot.tools.create_csv_store(output_folder=output_folder,
                                                   sep=csv_separator,
                                                   write_separate=split_outputs,
                                                   write_mode=write_mode)
    else:
        outputs = carrot.tools.create_sql_store()

    #build an object to store the cdm
    cdm = carrot.cdm.CommonDataModel(name=name,
                                        inputs=inputs,
                                        format_level=format_level,
                                        do_mask_person_id=not no_mask_person_id,
                                        outputs = outputs,
                                        #output_folder=output_folder,
                                        #output_database=output_database,
                                        automatically_fill_missing_columns=not dont_automatically_fill_missing_columns,
                                        use_profiler=use_profiler)
    #allow the csv separator to be changed
    #the default is tab (\t) separation
    #if not csv_separator is None:
    #    cdm.set_csv_separator(csv_separator)
    cdm.create_and_add_objects(config)

    cdm.process(conserve_memory=True)
    cdm.close()

    if merge_output:
        ctx.invoke(merge,
                   inputs=glob.glob(f"{output_folder}{os.path.sep}*"),
                   output_folder=output_folder)
    
@click.command(help="Perform OMOP Mapping given a python configuration file.")
@click.option("--rules",
              help="input json file containing all the mapping rules to be applied")
@click.option("--pyconf","--conf",
              required=True,
              help="Run with a python configuration file")
@click.option("--object","objects",
              multiple=True,
              help="Only run specific objects.")
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
def run_pyconfig(ctx,rules,pyconf,inputs,objects,
        output_folder,type,use_profiler,
        number_of_rows_per_chunk,
        number_of_rows_to_process):

    object_list = list(objects)
    if len(object_list) == 0:
        object_list = None

    if type != 'csv':
        raise NotImplementedError("Can only handle inputs that are .csv so far")
        
    #check if exists
    if any('*' in x for x in inputs):
        data_dir = os.path.dirname(carrot.__file__)
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
        
    outputs = carrot.tools.create_csv_store(output_folder=output_folder)


    if not inputs:
        raise Exception('no inputs defined!')

    inputs = tools.load_csv(inputs,
                            rules=rules,
                            chunksize=number_of_rows_per_chunk,
                            nrows=number_of_rows_to_process)

    available_classes = tools.get_classes()
    if pyconf not in available_classes:
        ctx.invoke(list_classes)
        raise KeyError(f"cannot find config {pyconf}. Run 'carrot map py list' to see available classes.")
    
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
              outputs=outputs,
              use_profiler=use_profiler)
    #run it
    cdm.process(object_list)
      
@click.command(help="run as a Graphical User Interface (GUI)")
@click.pass_context
def gui(ctx):
    import PySimpleGUI as sg

    carrot_theme = {'BACKGROUND': 'white',
                       'TEXT': '#000000',
                       'INPUT': '#c4c6e2',
                       'TEXT_INPUT': '#000000',
                       'SCROLL': '#c4c6e2',
                       'BUTTON': ('white', '#475da7','#3DB28C'),
                       'PROGRESS': ('#01826B', '#D0D0D0'),
                       'BORDER': 1,
                       'SLIDER_DEPTH': 1,
                       'PROGRESS_DEPTH': 0}
    
    # Add your dictionary to the PySimpleGUI themes
    sg.theme_add_new('carrot', carrot_theme)
    sg.theme('carrot')

    _dir = os.path.dirname(os.path.abspath(carrot.__file__))
    data_dir = f"{_dir}{os.path.sep}data{os.path.sep}"
    
    layout = [
        [sg.Image(f'{data_dir}logo.png'),sg.T("CO-CONNECT: Dataset2CDM",font = ("Roboto", 25))],
        [sg.T('Select the rules json:')],
        [sg.Input(key='_RULES_'), sg.FilesBrowse(initial_folder=os.getcwd())],
        [sg.T('Select the input CSVs:')],
        [sg.Input(key='_INPUTS_'), sg.FilesBrowse(initial_folder=os.getcwd())],
        [sg.T('Select an output folder:')],
        [sg.Input(key='_OUTPUT_',default_text='.'), sg.FolderBrowse(initial_folder=os.getcwd())],
        #[sg.Checkbox("Mask the person_id",key="_MASK_PERSON_ID_",default=False)],
        #[[sg.T('Change the default data chunksize:'),
        #  sg.Slider(range=(0,1000000),
        #            default_value=100000,
        #            resolution=10000,
        #            orientation='horizontal')]],
        [sg.OK('Run'), sg.Cancel(button_color=('white','#3DB28C'))]
    ]

    font = ("Roboto", 15)
    
    window = sg.Window('carrot', layout, font=font)
    while True:
        event, values = window.Read()
        
        if event == 'Cancel' or event == None:
            break

        output_folder = values['_OUTPUT_']
        if output_folder == '':
            output_folder = None
            
        rules = values['_RULES_']
        if rules == '':
            sg.Popup(f'Error: please select a rules file')
            continue
        elif len(rules.split(';'))>1:
            sg.Popup(f'Error: only select one file for the rules!')
            continue

        inputs = values['_INPUTS_']
        if inputs == '':
            sg.Popup(f'Error: please select at least one file or directory for the inputs')
            continue
        inputs = inputs.split(';')

        #mask_person_id = values['_MASK_PERSON_ID_']

        try:
            #ctx.invoke(run,rules=rules,inputs=inputs,output_folder=output_folder,mask_person_id=mask_person_id)
            ctx.invoke(run,rules=rules,inputs=inputs,output_folder=output_folder)
            sg.Popup("Done!")
        except Exception as err:
            sg.popup_error("An exception occurred!",err)
            
        break
        
    window.close()
    

@click.group(help="Commands for using python configurations to run the ETL transformation.")
def py():
    pass



py.add_command(make_class,"make")
py.add_command(register_class,"register")
py.add_command(list_classes,"list")
py.add_command(remove_class,"remove")
py.add_command(run_pyconfig,"map")
run.add_command(py,"py")
run.add_command(map,"map")
run.add_command(load,"load")
run.add_command(analysis,"analysis")
run.add_command(format,"format")
run.add_command(merge,"merge")
run.add_command(transform,"transform")
run.add_command(gui,"gui")
run.add_command(test,"test")
