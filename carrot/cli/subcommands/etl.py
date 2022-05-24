import click
import inquirer
import signal
import hashlib
import os
import glob
import subprocess
try:
    import daemon
    from daemon.pidfile import TimeoutPIDLockFile
except ImportError:
    #this package is not supported in Windows
    #latest version gives an import error of package pwd
    #https://stackoverflow.com/questions/39366261/python27win-import-daemon-but-there-is-an-error-no-module-named-pwd
    daemon = None

import lockfile
import shutil
import io
import time
import datetime
import yaml
import json
import copy
import pandas as pd

import carrot
from carrot.tools.bclink_helpers import BCLinkHelpers
from carrot.tools.logger import _Logger as Logger

from .run import map as cc_map
from .pseudonymise import pseudonymise

class PlatformNotSupported(Exception):
    pass

class UserNotSupported(Exception):
    pass

class DuplicateDataDetected(Exception):
    pass

class UnknownConfigurationSetting(Exception):
    pass

class BadConfigurationFile(Exception):
    pass

class MissingRulesFile(Exception):
    pass

class BadRulesFile(Exception):
    pass

def _load_config(config_file):
    stream = open(config_file) 
    config = yaml.safe_load(stream)
    return config

def _check_conf(config):
    if 'transform' not in config:
        raise BadConfigurationFile("You must specify a transform: block in your configration file")
    
    if 'data' not in config['transform']:
        raise BadConfigurationFile("You must specify a transform: data:  block in your configration file")
    #     raise MissingRulesFile(f"you must specify a json rules file in your '{config_file}'"
    #                            f" via 'rules:<path of file>'")
    
    # try:
    #     rules = carrot.tools.load_json(config['transform']['rules'])
    #     destination_tables = list(rules['cdm'].keys())
    # except Exception as e:
    #     raise BadRulesFile(e)

    #data = config['transform']['data']


def _outputs_exist(output,tables):
    return os.path.exists(output)

def _run_extract(config):
    output_folder = config['output']
    input_folder = config['input']
    files = carrot.tools.get_files(input_folder,type='csv')
    for f in files:
        bash_command = config['bash'].format(input=f,output=output_folder)
        for command in bash_command.splitlines():
            commands = command.split()
            if len(commands) == 0 :
                continue
            process = subprocess.Popen(command.split(), stdout=subprocess.PIPE)
            output, error = process.communicate()
            click.echo(output.decode("utf-8"))
    
    return output_folder


def _find_data(d):
    data = copy.deepcopy(d)
    root_input_folder = data.pop('input')
    root_output_folder = data.pop('output')
    additional = {}
    if isinstance(root_input_folder,dict):
        #there is some additional configuration to run here
        temp = root_input_folder.pop('input')
        additional = root_input_folder
        root_input_folder = temp
    subfolders = carrot.tools.get_subfolders(root_input_folder)
    data = [
        {**data,**{'input':f,'output':root_output_folder,'additional':additional}}
        for k,f in subfolders.items()
    ]
    return data

def _make_id(d):
    return hashlib.sha256(str(d).encode("utf-8")).hexdigest()

def _load_transform_data(data,processed_data={}):

    if isinstance(data,dict):
        data = _find_data(data)
    
    #extract the rules from the data
    data = {_make_id(d['input']):d for d in copy.deepcopy(data) }
    
    for _id,_data in data.items():
        processed_rules = processed_data[_id]['rules'] if _id in processed_data else None
        _data['rules'] = carrot.tools.load_json_delta(_data['rules'],processed_rules)
        
    return data

def _run_data(data,clean,ctx):
    logger = Logger("_run_data")
    _data = copy.deepcopy(data)
    rules = _data.pop('rules')
    if rules is None:
        logger.warning('no rules to run')
        return
    
    destination_tables = list(rules['cdm'].keys())

    
    input_folder = _data.pop('input')

    extract_config = _data.pop('additional',None)
    if extract_config:
        input_folder = _run_extract(extract_config)
    elif isinstance(input_folder,dict):
        input_folder = _run_extract(input_folder)
        
    inputs = carrot.tools.get_files(input_folder,type='csv')
    output = _data.pop('output')
    
    kwargs = {
        'split_outputs':True,
        'allow_missing_data':True,
        'write_mode':'a'
    }
    #assume the remained are kwargs for the transform
    kwargs.update(_data)


    output_database = None
    output_folder = None
    if isinstance(output,dict):
        output_database = output
        if clean:
            ctx.invoke(clean_tables)
    else:
        ## get output folder
        output_folder = output
        if _outputs_exist(output_folder,destination_tables):
            logger.warning(f'{output_folder} exists!')
            if clean and os.path.exists(output_folder) and os.path.isdir(output_folder):
                logger.warning(f"removing {output_folder}")
                shutil.rmtree(output_folder)

    #invoke mapping
    try:
        ctx.invoke(cc_map,
                   rules=rules,
                   inputs=inputs,
                   output_folder=output_folder,
                   output_database=output_database,
                   **kwargs
        )
    except carrot.cdm.model.PersonExists as e:
        logger.error(e)
        logger.error(f"failed to map {inputs} because there were people")
        logger.error(f" already processed and present in existing data. Check the person_id map/lookup!")
    return True



def _run_etl(ctx,config_file):
    logger = Logger("run_etl")
    last_modified_config = os.path.getmtime(config_file)
    logger.info(f"running etl on {config_file} (last modified: {last_modified_config})")

    conf = _load_config(config_file)
    _check_conf(conf)
    data = _load_transform_data(conf['transform']['data'])

    ctx.obj = {'conf':conf,'data':data}
    if not ctx.invoked_subcommand == None :
        return

    settings = conf.get('settings',{})
    listen_for_changes = settings.get('listen_for_changes',False)
    clean = settings.get('clean',False)
    
    #run the data
    _ = [_run_data(d,clean if i==0 else False,ctx) for i,d in enumerate(data.values())]
        
    display_msg = True
    while True:
        #if a change has been detected in the config_file
        #load it up again
        if last_modified_config !=  os.path.getmtime(config_file):
            conf = _load_config(config_file)
            _check_conf(conf)
            last_modified_config = os.path.getmtime(config_file)
        
        #reload to detect if there's something different going on
        new_data = _load_transform_data(conf['transform']['data'],data)
        #work out what needs to be re-processed 
        data_to_process = {
            k:v
            for k,v in new_data.items()
            if not (k in data and v==data[k])
        }
        
        display_msg = True if data_to_process else display_msg
        #run the data
        _ = [_run_data(d,False,ctx) for d in data_to_process.values()]

        #update the data to 
        data = _load_transform_data(conf['transform']['data'])

        if not listen_for_changes:
            break
        
        if display_msg:
            logger.info(f"Finished!... Listening for changes every 5 seconds to data in {config_file}")
            display_msg = False
    
        time.sleep(5)




@click.group(help='Command group for running the full ETL of a dataset',invoke_without_command=True)
@click.option('config_file','--config','--config-file',help='specify a yaml configuration file')
@click.option('run_as_daemon','--daemon','-d',help='run the ETL as a daemon process',is_flag=True)
@click.option('--log-file','-l',default='carrot.log',help='specify the log file to write to')
@click.pass_context
def etl(ctx,config_file,run_as_daemon,log_file):
    logger = Logger("etl")
    
    if run_as_daemon and daemon is None:
        raise ImportError(f"You are trying to run in daemon mode, "
                          "but the package 'daemon' hasn't been installed. "
                          "pip install python-daemon. \n"
                          "If you are running on a Windows machine, this package is not supported")

    if run_as_daemon and daemon is not None:
        stderr = log_file
        stdout = f'{stderr}.out'
     
        logger.info(f"running as a daemon process, logging to {stderr}")
        pidfile = TimeoutPIDLockFile('etl.pid', -1)
        logger.info(f"process_id in {pidfile}")

        with open(stdout, 'w+') as stdout_handle, open(stderr, 'w+') as stderr_handle:
            d_ctx = daemon.DaemonContext(
                working_directory=os.getcwd(),
                stdout=stdout_handle,
                stderr=stderr_handle,
                pidfile=TimeoutPIDLockFile('etl.pid', -1)
            )
            with d_ctx:
                _run_etl(ctx,config_file)
    else:
        _run_etl(ctx,config_file)





@click.group(help='Command group for ETL integration with bclink')
@click.option('--force','-f',help='Force running of this, useful for development purposes',is_flag=True)
@click.option('--interactive','-i',help='run with interactive options - i.e. so user can confirm operations',is_flag=True)
@click.option('config_file','--config','--config-file',help='specify a yaml configuration file',required=True)
@click.pass_context
def bclink(ctx,force,config_file,interactive):
    user = os.environ.get("USER")

    if not force:
        #check the platform (i.e. should be centos)
        if os.name == 'nt':
            raise PlatformNotSupported(f"Not suported to run this on Windows")
        #check the username
        #for bclink, we need to run as bcos_srv to get access to all the datasettool2 etc. tools
        #and be able to connect with the postgres server without the need for a password
        if user != 'bcos_srv':
            raise UserNotSupported(f"{user} not supported! You must run this as user 'bcos_srv'")
        
    config = _load_config(config_file)

    m_steps = ['clean','extract','transform','load']
    #put in protection for missing keys
    if 'rules' not in config:
        raise MissingRulesFile(f"you must specify a json rules file in your '{config_file}'"
                               f" via 'rules:<path of file>'")

    try:
        rules = carrot.tools.load_json(config['rules'])
        destination_tables = list(rules['cdm'].keys())
    except Exception as e:
        raise BadRulesFile(e)
    
    bclink_settings = {}
    if 'bclink' in config:
        bclink_settings = config.pop('bclink')
    elif user != 'bcos_srv':
        bclink_settings['dry_run'] = True
        m_steps.remove('clean')
        m_steps.remove('extract')

    if 'tables' not in bclink_settings:
        bclink_settings['tables'] = {x:x for x in destination_tables}

    if 'global_ids' not in bclink_settings:
        #change default behaviour to non-table
        bclink_settings['global_ids'] = None#'global_ids'

    bclink_settings['tables'] = _get_table_map(bclink_settings['tables'],destination_tables)
    bclink_helpers = BCLinkHelpers(**bclink_settings)
    if ctx.obj is None:
        ctx.obj = dict()
    ctx.obj['bclink_helpers'] = bclink_helpers
    ctx.obj['rules'] = rules
    ctx.obj['data'] = config['data']
   
    log = 'carrot.log'
    if 'log' in config.keys():
        log = config['log']
    ctx.obj['log'] = log
    ctx.obj['conf'] = config_file

    clean = False
    if 'clean' in config.keys():
        clean = config['clean']
    ctx.obj['clean'] = clean

    ctx.obj['interactive'] = interactive
    
    if 'pseudonymise' in config:
        ctx.obj['pseudonymise'] = config['pseudonymise']

    #define the default steps to execute
    ctx.obj['steps'] = m_steps

    unknown_keys = list( set(config.keys()) - set(ctx.obj.keys()) )
    if len(unknown_keys) > 0 :
        raise UnknownConfigurationSetting(f'"{unknown_keys}" are not valid settings in the yaml file')

    if ctx.invoked_subcommand == None:
        ctx.invoke(execute)


def _process_data(ctx):
    data = ctx.obj['data']
    if isinstance(data,list):
        _process_list_data(ctx)
    else:
        _process_dict_data(ctx)
    
def _process_list_data(ctx):
    logger = Logger("_process_list_data")
    logger.info("ETL process has begun")
   
    interactive = ctx.obj['interactive']
    data = []
    clean = ctx.obj['clean']
    rules = ctx.obj['rules']
    bclink_helpers = ctx.obj['bclink_helpers']
    config_file = ctx.obj['conf']
    conf = _load_config(config_file)
    rules_file = conf['rules']
    rules_file_last_modified = os.path.getmtime(rules_file) 
    
    bclink_helpers.print_summary()
    display_msg = True
    _clean = clean

    while True:
        
        re_execute = False
        try:
            conf = _load_config(config_file)
        except Exception as e:
            if not display_msg:
                logger.critical(e)
                logger.error(f"You've misconfigured your file '{config_file}'!! Please fix!")
            time.sleep(5)
            display_msg = True
            continue

        current_rules_file = conf['rules']
        new_rules_file = rules_file != current_rules_file

        if new_rules_file:
            #if there's a new rules file
            logger.info(f"Detected a new rules file.. old was '{rules_file}' and new is '{current_rules_file}'")
            rules_file = current_rules_file
            rules = carrot.tools.load_json_delta(rules_file,rules)
            rules_file_last_modified = os.path.getmtime(rules_file)
            re_execute = True
        else: 
            #otherwise check for changes in the existing file
            new_rules_file_last_modified = os.path.getmtime(current_rules_file)
            change_in_rules = rules_file_last_modified != new_rules_file_last_modified
            if change_in_rules:
                logger.info(f"Detected a change/update in the rules file '{rules_file}'")
                rules = carrot.tools.load_json_delta(current_rules_file,rules)
                re_execute = True 
            
        current_data = conf['data']
        print (data)
        print (current_data)
        exit(0)
        if not data == current_data:
            logger.debug(f"old {data}")
            logger.debug(f"new {current_data}")
            new_data = [obj for obj in current_data if obj not in data]
            logger.info(f"New data found! {new_data}")
            re_execute = True
        else:
            new_data = data

        logger.debug(f"re-execute {re_execute}")
        print (re_execute)
        exit(0)
        if re_execute:
            current_data = copy.deepcopy(new_data)
            #loop over any new data
            for item in new_data:
                if isinstance(item['input'],list):
                    inputs = item['input']
                else:
                    input_folder = item['input']
                    if not os.path.isdir(input_folder):
                        raise Exception(f"{input_folder} is not a directory containing files!")
                    inputs = carrot.tools.get_files(input_folder,type='csv')
                filtered_rules = carrot.tools.remove_missing_sources_from_rules(rules,inputs)

                _execute(ctx,
                         data=item,
                         rules=filtered_rules,
                         clean=_clean
                     )
                _clean = False
            
            data += [x for x in current_data if x not in data]
            display_msg=True
       

        if new_rules_file or change_in_rules:
            #if there's a new rules file or rules delta,
            #need to pick up the full rules for the next loop
            #incase we insert new data
            # --> we dont want to just apply the delta to the new data
            rules = carrot.tools.load_json(current_rules_file)
       
        if ctx.obj['listen_for_changes'] == False:
            break
    
        if display_msg:
            logger.info(f"Finished!... Listening for changes to data in {config_file}")
            if display_msg:
                display_msg = False
    
        time.sleep(5)
        

def _process_dict_data(ctx):
    logger = Logger("_process_dict_data")
    logger.info("ETL process has begun")

    interactive = ctx.obj['interactive']
    data = ctx.obj['data']
    clean = ctx.obj['clean']
    rules = ctx.obj['rules']
    bclink_helpers = ctx.obj['bclink_helpers']
    
    bclink_helpers.print_summary()

    #calculate the amount of time to wait before checking for changes
    tdelta = None
    if 'watch' in data:
        watch = data['watch']
        tdelta = datetime.timedelta(**watch)
        
    #get the input folder to watch
    input_folder = data['input']
    #get the root output folder
    output_folder = data['output']

                
    i = 0
    while True:
        #find subfolders containing data dumps
        subfolders = carrot.tools.get_subfolders(input_folder)
        # if len(subfolders)>0:
        #     logger.info(f"Found {len(subfolders)} subfolders at path '{input_folder}'")
        # if interactive and len(subfolders)>0:
        #     questions = [
        #         inquirer.Checkbox('folders',
        #                           message="Confirm processing the following subfolders.. ",
        #                           choices=subfolders,
        #                           default=subfolders
        #                           )
        #         ]
        #     answers = inquirer.prompt(questions)
        #     if answers == None:
        #         os.kill(os.getpid(), signal.SIGINT)
            
        #     subfolders = {k:v for k,v in subfolders.items() if k in answers['folders']}
        #     logger.info(f"selected {subfolders}")
        
        logger.debug(f"Found and checking {len(subfolders.values())} subfolders")
        logger.debug(list(subfolders.values()))
  
        if len(subfolders.values())> 0:
            logger.debug(f"{list(subfolders.values())}")
                  
        njobs=0
        #print (reversed(sorted(subfolders.items(),key=lambda x: os.path.getmtime(x[1]))))
        for name,path in sorted(subfolders.items(),key=lambda x: os.path.getmtime(x[1])):
            output_folder_exists = os.path.exists(f"{output_folder}/{name}")
  
            inputs = carrot.tools.get_files(path,type='csv')
            filtered_rules = carrot.tools.remove_missing_sources_from_rules(rules,inputs)

            if output_folder_exists:
                output_tables = [
                    os.path.splitext(os.path.basename(x))[0]
                    for x in carrot.tools.get_files(f"{output_folder}/{name}",type='tsv')
                ]
                
                expected_outputs = list(filtered_rules['cdm'].keys())
                to_process = list(set(expected_outputs) - set(output_tables))
                
                if len(to_process) == 0:
                    continue

                filtered_rules = carrot.tools.filter_rules_by_destination_tables(filtered_rules,to_process)
               

            logger.debug(f"New data found!")
            logger.info(f"Creating a new task for processing {path}")
                
                            
            if len(inputs) == 0:
                logger.critical(f"Subfolder contains no .csv files!")
                continue
                    
            tables = list(filtered_rules['cdm'].keys())
            logger.debug(f'inputs: {inputs}')
            logger.info(f'cdm tables: {tables}')
                
  
            _data = copy.deepcopy(data)
            _data['input'] = inputs
            _data['output'] = f"{output_folder}/{name}"
        

            _execute(ctx,
                     data=_data,
                     rules=filtered_rules,
                     clean=clean if (i==0 and njobs==0) else False
            )
            njobs+=1
            
        if tdelta is None:
            break
                
        if njobs>0 or i==0:
            logger.info(f"Refreshing {input_folder} every {tdelta} to look for new subfolders....")
            if len(subfolders.values()) == 0:
                logger.warning("No subfolders for data dumps yet found...")

        i+=1
        time.sleep(tdelta.total_seconds())

@click.command(help='print all tables in the bclink tables defined in the config file')
@click.option('--drop-na',is_flag=True)
@click.option('--markdown',is_flag=True)
@click.option('--head',type=int,default=None)
@click.argument("tables",nargs=-1)
@click.pass_obj
def print_tables(ctx,drop_na,markdown,head,tables):

    bclink_helpers = ctx['bclink_helpers']
    logger = Logger("print_tables")

    tables_to_print = list(tables)
    if len(tables_to_print) == 0:
        tables_to_print = list(bclink_helpers.table_map.keys())

    tables = [
        table 
        for table_name,table in bclink_helpers.table_map.items()
        if table_name in tables_to_print
    ]

    for table in tables:
        df = bclink_helpers.get_table(table)
        df.set_index(df.columns[0],inplace=True)
        if drop_na:
            df = df.dropna(axis=1,how='all')
        if markdown:
            df = df.to_markdown()

        click.echo(df)


@click.command(help='delete some tables')
@click.pass_obj
def delete_tables(ctx):
    logger = Logger('delete_tables')
    out_folders = []
    bc_settings = None
    for item in ctx['data'].values():
        output = item['output']
        if isinstance(output,str):
            pass
        elif 'bclink' in output:
            bc_settings = output['bclink']
            output = output['cache']
            
        if os.path.exists(output) and os.path.isdir(output):
            out_folders.append(output)

    out_folders = list(set(out_folders))
    all_files = [
        carrot.tools.get_files(f,type='tsv')
        for f in out_folders
    ]
    all_files = [ subitem for item in all_files for subitem in item]

    choices = all_files
    questions = [
        inquirer.Checkbox('files',
                          message=f"Which tables do you want to delete? ... ",
                          choices=choices,
                          default=[],
        )
    ]
    answers = inquirer.prompt(questions)
    if answers == None:
        os.kill(os.getpid(), signal.SIGINT)

    files_to_delete = answers['files']
    if bc_settings:
        helpers = BCLinkHelpers(**bc_settings)
        for f in files_to_delete:
            helpers.remove_table(f)
    
    for f in files_to_delete:
        logger.warning(f"removing {f}")
        os.remove(f)

@click.command(help='create new bclink tables')
@click.option('--tables',help="specify which tables to remove",default=None)
@click.pass_obj
def create_tables(ctx,tables):
    logger = Logger("check-tables")
    load = ctx['conf']['load']
    if 'bclink' in load:
        settings = load['bclink']
        settings['clean'] = False
        settings['check'] = False
        if tables:
            settings['tables'] = {k:v for k,v in settings['tables'].items() if k in tables}
        helpers = BCLinkHelpers(**settings)
        helpers.create_tables()
    else:
        raise NotImplementedError("cannot clean tables for configuration load: {load}")
  

@click.command(help='check tables')
@click.option('--tables',help="specify which tables to remove",default=None)
@click.pass_obj
def check_tables(ctx,tables):
    logger = Logger("check-tables")
    for item in ctx['data'].values():
        output = item['output']
        logger.info(f"cleaning {output}")
        if 'bclink' in output:
            settings = output['bclink']
            settings['clean'] = False
            if tables:
                settings['tables'] = {k:v for k,v in settings['tables'].items() if k in tables}
            helpers = BCLinkHelpers(**settings)
        else:
            raise NotImplementedError(f"cannot call check_tables on output {output}. This is for bclink stuff")
            


@click.command(help='clean (delete all rows) of a given table name')
@click.argument('table')
@click.pass_context
def clean_table(ctx,table):
    ctx.invoke(clean_tables,tables=[table])

@click.command(help='clean (delete all rows) in the tables defined in the config file')
@click.option('--tables',help="specify which tables to remove",default=None)
@click.pass_obj
def clean_tables(ctx,tables):
    logger = Logger("clean-tables")
    load = ctx['conf']['load']

    if 'bclink' in load:
        settings = load['bclink']
        settings['clean'] = True
        if tables:
            settings['tables'] = {k:v for k,v in settings['tables'].items() if k in tables}
        helpers = BCLinkHelpers(**settings)
    else:
        raise NotImplementedError("cannot clean tables for configuration load: {load}")
         

@click.command(help='delete data that has been inserted into bclink')
@click.pass_obj
def delete_data(ctx):
    bclink_helpers = ctx['bclink_helpers']
    logger = Logger("delete_data")
    logger.info("deleting data...")

    data = ctx['data']
    input_data = data['input']
    output_data = data['output']
    
    
    folders = carrot.tools.get_subfolders(output_data)
    
    options = [
        inquirer.Checkbox('folders',
                      message="Which data-dump do you want to remove?",
                      choices=list(folders.values())
            ),
    ]
    selected_folders = inquirer.prompt(options)
    if selected_folders == None:
        os.kill(os.getpid(), signal.SIGINT)
    selected_folders = selected_folders["folders"]

    for selected_folder in selected_folders:
        files = carrot.tools.get_files(selected_folder,type='tsv')
                
        options = [
            inquirer.Checkbox('files',
                              message="Confirm the removal of the following tsv files.. ",
                              choices=files,
                              default=files
                          ),
        ]
        selected_files = inquirer.prompt(options)
        if selected_files == None:
            os.kill(os.getpid(), signal.SIGINT)
        selected_files = selected_files["files"]

        for f in selected_files:
        
            bclink_helpers.remove_table(f)
    
            click.echo(f"Deleting {f}")
            os.remove(f)

    
@click.command(help='check and drop for duplicates')
@click.pass_obj
def drop_duplicates(ctx):
    bclink_helpers = ctx['bclink_helpers']
    logger = Logger("drop_duplicates")

    retval = {}
    logger.info("printing to see if tables exist")
    for cdm_table,bclink_table in bclink_helpers.table_map.items():
        #dont do this for person table
        #a person with the same sex and date of birth isnt a duplicate
        if cdm_table == "person":
            continue
        logger.info(f"Looking for duplicates in {cdm_table} ({bclink_table})")

        #if the table hasnt been created, skip
        exists = bclink_helpers.check_table_exists(bclink_table)
        if not exists:
            continue
        #find out what the primary key is 
        droped_duplicates = bclink_helpers.drop_duplicates(bclink_table)
        if len(droped_duplicates)>0:
            logger.warning(f"Found and dropped {len(droped_duplicates)} duplicates in {bclink_table}")
            
@click.command(help='Run the Extract part of ETL process for CO-CONNECT integrated with BCLink')
@click.pass_context
def extract(ctx):
    logger = Logger("Extract")
    logger.info("doing extract only")
    ctx.obj['steps'] = ['extract']
    ctx.invoke(execute)

@click.command(help='Run the Transform part of ETL process for CO-CONNECT integrated with BCLink')
@click.pass_context
def transform(ctx):
    logger = Logger("Transform")
    logger.info("doing transform only")
    ctx.obj['steps'] = ['transform']
    ctx.invoke(execute)

@click.command(help='Run the Load part of ETL process for CO-CONNECT integrated with BCLink')
@click.pass_context
def load(ctx):
    logger = Logger("Load")
    logger.info("doing load only")
    ctx.obj['steps'] = ['load']
    ctx.invoke(execute)


@click.command(help='Run the full ETL process for CO-CONNECT integrated with BCLink')
@click.option('run_as_daemon','--daemon','-d',help='run the ETL as a daemon process',is_flag=True)
@click.pass_context
def execute(ctx,run_as_daemon):
    logger = Logger("Execute")
    
    if run_as_daemon and daemon is None:
        raise ImportError(f"You are trying to run in daemon mode, "
                          "but the package 'daemon' hasn't been installed. "
                          "pip install python-daemon. \n"
                          "If you are running on a Windows machine, this package is not supported")

    if run_as_daemon and daemon is not None:
        stderr = ctx.obj['log']
        stdout = f'{stderr}.out'
     
        logger.info(f"running as a daemon process, logging to {stderr}")
        pidfile = TimeoutPIDLockFile('etl.pid', -1)
        logger.info(f"process_id in {pidfile}")

        with open(stdout, 'w+') as stdout_handle, open(stderr, 'w+') as stderr_handle:
            d_ctx = daemon.DaemonContext(
                working_directory=os.getcwd(),
                stdout=stdout_handle,
                stderr=stderr_handle,
                pidfile=TimeoutPIDLockFile('etl.pid', -1)
            )
            with d_ctx:
                _process_data(ctx)
    else:
        _process_data(ctx)


def _extract(ctx,data,rules,bclink_helpers):
    if not 'extract' in ctx.obj['steps']:
        return {'data':data}

    logger = Logger("extract")
    logger.info(f"starting extraction processes")

    inputs = data['input']
    if isinstance(inputs,str):
        if not os.path.exists(inputs):
            raise Exception(f"{inputs} is not an existing path")
        if not os.path.isdir(inputs):
             raise Exception(f"{inputs} is not a dir!")
        inputs = carrot.tools.get_files(inputs)
        if len(inputs) == 0:
            raise Exception(f"No .csv files found in {inputs}")
    
    do_pseudonymise=False
    _pseudonymise = {}
    if 'pseudonymise' in data:
        _pseudonymise = data['pseudonymise']
        do_pseudonymise = True
        if 'do' in _pseudonymise:
            do_pseudonymise = _pseudonymise['do']
    
    if do_pseudonymise:        
        chunksize = 1000
        if 'chunksize' in _pseudonymise:
            chunksize = _pseudonymise['chunksize']

        output = "./pseudonymised_input_data/"
        if 'output' in _pseudonymise:
            output = _pseudonymise['output']

        if 'salt' not in _pseudonymise:
            raise Exception("To use pseudonymise a salt must be provided!")
        salt = _pseudonymise['salt']
                
        logger.info(f"Called do_pseudonymisation on input data {data} ")
        if not isinstance(rules,dict):
            rules = carrot.tools.load_json(rules)
        person_id_map = carrot.tools.get_person_ids(rules)

        input_map = {os.path.basename(x):x for x in inputs}

        inputs = []
        for table,person_id in person_id_map.items():
            if table not in input_map:
                logger.warning(f"Could not find table {table} in input_map")
                logger.warning(input_map)
                continue
            fin = input_map[table]

            fout = ctx.invoke(pseudonymise,
                              input=fin,
                              output_folder=output,
                              chunksize=chunksize,
                              salt=salt,
                              person_id=person_id
                          )
            inputs.append(fout)
        
        data.pop('pseudonymise')
        data['input'] = inputs
    

    _dir = data['output']
 
    f_global_ids = f"{_dir}/existing_global_ids.tsv"
    f_global_ids = bclink_helpers.get_global_ids(f_global_ids)
    if f_global_ids == None:
        f_global_ids = f"{_dir}/global_ids.tsv"
        if not os.path.exists(f_global_ids):
            f_global_ids = None
        else:
            logger.warning(f"falling back to {f_global_ids} for global_ids")
    
    indexer = bclink_helpers.get_indicies()
    return {
        'indexer':indexer,
        'data':data,
        'existing_global_ids':f_global_ids
    }

def _transform(ctx,rules,inputs,output_folder,indexer,existing_global_ids,**kwargs):
    if not 'transform' in ctx.obj['steps']:
        return 

    logger = Logger("transform")
    logger.info("starting data transform processes")

    if isinstance(inputs,str):
        inputs = [inputs]

    logger.info(f"inputs: {inputs}")
    logger.info(f"output_folder: {output_folder}")
    logger.info(f"indexer: {indexer}")
    logger.info(f"existing_global_ids: {existing_global_ids}")
    
    ctx.invoke(cc_map,
               rules=rules,
               inputs=inputs,
               output_folder=output_folder,
               indexing_conf=indexer,
               person_id_map=existing_global_ids,
               **kwargs
    ) 

def _load(ctx,output_folder,cdm_tables,global_ids,bclink_helpers):

    if not 'load' in ctx.obj['steps']:
        return 

    logger = Logger("load")
    logger.info("starting loading data processes")

    logger.info("starting loading global ids")
    if global_ids:
        bclink_helpers.load_global_ids(output_folder)
        
    logger.info("starting loading cdm tables")
    bclink_helpers.load_tables(output_folder,cdm_tables)

        
def _execute(ctx,
             rules=None,
             data=None,
             clean=None,
             bclink_helpers=None):
    
    if data == None:
        data = ctx.obj['data']
    if clean == None:
        clean = ctx.obj['clean']
    if rules == None:
        rules = ctx.obj['rules']
    if bclink_helpers == None:
        bclink_helpers = ctx.obj['bclink_helpers']
    
    interactive = ctx.obj['interactive']
    steps = ctx.obj['steps']

    ctx.obj['listen_for_changes'] = all([step in steps for step in ['extract','transform','load']])

    check_and_drop_duplicates = 'drop_duplicates' in steps

    logger = Logger("execute")
    logger.info(f"Executing steps {steps}")
   

    if clean and 'clean' in steps:
        logger.info(f"cleaning existing bclink tables")
        ctx.invoke(clean_tables,data=data)
   

    tables = list(rules['cdm'].keys())
    if interactive and ('extract' in steps or 'transform' in steps):
        choices = []
        #location = f"{output_folder}/{name}"
        for table in tables:
            source_tables = [
                f"{data['input']}/{x}"
                for x in carrot.tools.get_source_tables_from_rules(rules,table)
            ]
            choices.append((f"{table} ({source_tables})",table))
        questions = [
            inquirer.Checkbox('tables',
                              message=f"Confirm executing ETL for ... ",
                              choices=choices,
                              default=tables
                          )
        ]
        answers = inquirer.prompt(questions)
        if answers == None:
            os.kill(os.getpid(), signal.SIGINT)
        tables = answers['tables']
        if len(tables) == 0:
            logger.info("no tables selected, skipping..")
            return
        rules = carrot.tools.filter_rules_by_destination_tables(rules,tables)
        logger.info(f'cdm tables: {tables}')
        
    logger.info(f"Executing ETL...")
        
    #call any extracting of data
    #----------------------------------
    extract_data= _extract(ctx,
                           data,
                           rules,
                           bclink_helpers
    ) 
    indexer = extract_data.get('indexer')
    existing_global_ids = extract_data.get('existing_global_ids')
    data = extract_data.get('data')
   
    #----------------------------------

    inputs = data.pop('input')
    output_folder = data.pop('output')
    transform_kwargs = data
    
    #call transform
    #----------------------------------
    _transform(ctx,
               rules,
               inputs,
               output_folder,
               indexer,
               existing_global_ids,
               **transform_kwargs
    )
    #----------------------------------       
    #remove this lookup file once done with it
    if existing_global_ids and os.path.exists(existing_global_ids):
        os.remove(existing_global_ids)


    if 'load' not in steps:
        logger.info("done!")
        return

    cdm_tables = carrot.tools.get_files(output_folder,type='tsv')
    if interactive:
        choices = []
        for x in cdm_tables:
            tab = os.path.splitext(os.path.basename(x))[0]
            bctab = bclink_helpers.get_bclink_table(tab)
            text = f"{x} --> {bctab} ({tab})"
            choices.append((text,x))
        options = [
            inquirer.Checkbox('cdm_tables',
                              message="Choose which CDM tables to load..",
                              choices=choices,
                              default=cdm_tables
                          ),
        ]
        answers = inquirer.prompt(options)
        if answers == None:
            os.kill(os.getpid(), signal.SIGINT)
        tables_to_load = answers['cdm_tables']
        cdm_tables = tables_to_load
        if len(cdm_tables) == 0 :
            logger.info("No tables chosen to be loaded..")
            return
        else:
            logger.info("Chosen to load...")
            logger.warning(cdm_tables)

    cdm_tables = [
        os.path.splitext(os.path.basename(x))[0]
        for x in cdm_tables
    ]
        
    try:
        idx_global_ids = cdm_tables.index('global_ids')
        global_ids = cdm_tables.pop(idx_global_ids)
    except ValueError:
        global_ids = None
    
    #call load
    #----------------------------------        
    _load(ctx,
          output_folder,
          cdm_tables,
          global_ids,
          bclink_helpers
    )

    if check_and_drop_duplicates:
        #final check for duplicates
        logger.info(f"looking for duplicates and deleting any")
        ctx.invoke(drop_duplicates)

    bclink_helpers.print_report()
    logger.info("done!")


def _get_table_map(table_map,destination_tables):
    #if it's not a dict, and is a file, load the json
    if not isinstance(table_map,dict):
        table_map = carrot.tools.load_json(table_map)

    # loop over all tables from the rules json
    for table_name in destination_tables:
        #if the dest table is not in the mapping, fail
        if table_name not in table_map.keys():
            raise Exception(f"You must give the name of the bclink table for {table_name}")

    #drop any tables that are not mapped (not in the destination_tables)
    table_map = {k:v for k,v in table_map.items() if k in destination_tables}
    return table_map
    
@click.command(help='[for developers] Run the CO-CONNECT ETL manually ')
@click.option('--rules','-r',help='location of the json rules file',required=True)
@click.option('--output-folder','-o',help='location of the output results folder',required=True)
@click.option('--clean',help='clean all the BCLink tables first by removing all existing rows',is_flag=True)
@click.option('--table-map','-t',help='a look up json file that maps between the CDM table and the table name in BCLink',default={})
@click.option('--gui-user',help='name of the bclink gui user',default='data')
@click.option('--user',help='name of the bclink user',default='bclink')
@click.option('--database',help='name of the bclink database',default='bclink')
@click.option('--dry-run',help='peform a dry-run of the bclink uplod',is_flag=True)
@click.argument('inputs',required=True,nargs=-1)
@click.pass_context
def manual(ctx,rules,inputs,output_folder,clean,table_map,gui_user,user,database,dry_run):

    _rules = carrot.tools.load_json(rules)
    destination_tables = list(_rules['cdm'].keys())
    
    data = {
        'input':list(inputs),
        'output':output_folder
    }

    table_map = _get_table_map(table_map,destination_tables)
    bclink_settings = {
        'user':user,
        'gui_user': gui_user,
        'database':database,
        'dry_run':dry_run,
        'tables':table_map,
    }

    logger = Logger("Manual")
    logger.info(f'Rules: {rules}')
    logger.info(f'Inputs: {data["input"]}')
    logger.info(f'Output: {data["output"]}')
    logger.info(f'Clean Tables: {clean}')
    logger.info(f'Processing {destination_tables}')
    logger.info(f'BCLink settings:')
    logger.info(json.dumps(bclink_settings,indent=6))
    
    bclink_helpers = BCLinkHelpers(**bclink_settings)

    _execute(ctx,rules,data,clean,bclink_helpers)

                

#bclink.add_command(print_tables,'print_tables')
#bclink.add_command(clean_tables,'clean_tables')
#bclink.add_command(delete_data,'delete_data')
#bclink.add_command(drop_duplicates,'drop_duplicates')
#bclink.add_command(check_tables,'check_tables')
#bclink.add_command(create_tables,'create_tables')
#bclink.add_command(execute,'execute')
#bclink.add_command(extract,'extract')
#bclink.add_command(transform,'transform')
#bclink.add_command(load,'load')
#etl.add_command(manual,'bclink-manual')
#etl.add_command(bclink,'bclink')
etl.add_command(create_tables,'create-tables')
etl.add_command(check_tables,'check-tables')
etl.add_command(clean_table,'clean-table')
etl.add_command(clean_tables,'clean-tables')
etl.add_command(delete_tables,'delete-tables')



