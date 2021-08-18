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
    _dir = os.path.dirname(os.path.abspath(coconnect.__file__))
    _dir = f"{_dir}{os.path.sep}data{os.path.sep}test"
    inputs = glob.glob(f"{_dir}{os.path.sep}inputs{os.path.sep}*.csv")
    rules = f"{_dir}{os.path.sep}rules{os.path.sep}rules_14June2021.json"
    output_folder = 'tests'
    ctx.invoke(run,inputs=inputs,rules=rules,output_folder=output_folder)
    for fname in glob.glob(f"{output_folder}{os.path.sep}*.tsv"):
        tools.diff_csv(fname,fname)
          
@click.command()
@click.option("--rules",
              required=True,
              help="input json file containing all the mapping rules to be applied")
@click.option("--csv-separator",
              default=None,
              type=click.Choice([';',':','\t',',',' ',]),
              help="choose a separator to use when dumping output csv files")
@click.option("--use-profiler",
              is_flag=True,
              help="turn on saving statistics for profiling CPU and memory usage")
@click.option("--output-folder",
              default=None,
              help="define the output folder where to dump csv files to")
@click.option("-nc","--number-of-rows-per-chunk",
              default='auto',
              help="Choose the number of rows (INTEGER) of input data to load (chunksize). The default 'auto' will work out the ideal chunksize. Inputing a value <=0 will turn off data chunking.")
@click.option("-np","--number-of-rows-to-process",
              default=None,
              type=int,
              help="the total number of rows to process")
@click.argument("inputs",
                required=True,
                nargs=-1)
@click.pass_context
def run(ctx,rules,inputs,
        output_folder,csv_separator,use_profiler,
        number_of_rows_per_chunk,
        number_of_rows_to_process):
    """
    Perform OMOP Mapping given an json file and a series of input files

    INPUTS should be a space separated list of individual input files or directories (which contain .csv files)
    """

    #load the json loads
    config = tools.load_json(rules)
    name = config['metadata']['dataset']

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

    #build an object to store the cdm
    cdm = coconnect.cdm.CommonDataModel(name=name,
                                        inputs=inputs,
                                        output_folder=output_folder,
                                        use_profiler=use_profiler)
    
    #allow the csv separator to be changed
    #the default is tab (\t) separation
    if not csv_separator is None:
        cdm.set_csv_separator(csv_separator)
    
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
    #run it
    cdm.process()
      
@click.command(help="run as a Graphical User Interface (GUI)")
@click.pass_context
def gui(ctx):
    import PySimpleGUI as sg

    coconnect_theme = {'BACKGROUND': 'white',
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
    sg.theme_add_new('coconnect', coconnect_theme)
    sg.theme('coconnect')

    _dir = os.path.dirname(os.path.abspath(coconnect.__file__))
    data_dir = f"{_dir}{os.path.sep}data{os.path.sep}"
    
    layout = [
        [sg.Image(f'{data_dir}logo.png'),sg.T("CO-CONNECT ETL-Tool",font = ("Roboto", 25))],
        [sg.T('Select the rules json:')],
        [sg.Input(key='_RULES_'), sg.FilesBrowse(initial_folder=os.getcwd())],
        [sg.T('Select the input CSVs:')],
        [sg.Input(key='_INPUTS_'), sg.FilesBrowse(initial_folder=os.getcwd())],
        [sg.T('Select an output folder:')],
        [sg.Input(key='_OUTPUT_',default_text='.'), sg.FolderBrowse(initial_folder=os.getcwd())],
        #[[sg.T('Change the default data chunksize:'),
        #  sg.Slider(range=(0,1000000),
        #            default_value=100000,
        #            resolution=10000,
        #            orientation='horizontal')]],
        [sg.OK('Run'), sg.Cancel(button_color=('white','#3DB28C'))]
    ]

    font = ("Roboto", 15)
    
    window = sg.Window('COCONNECT', layout, font=font)
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
        ctx.invoke(run,rules=rules,inputs=inputs,output_folder=output_folder)
        sg.Popup("Done!")
        break
        
    window.close()

    

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
map.add_command(gui,"gui")
map.add_command(test,"test")
