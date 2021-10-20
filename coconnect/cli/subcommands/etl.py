import click
import inquirer
import signal

import os
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

import coconnect
from coconnect.tools.bclink_helpers import BCLinkHelpers
from coconnect.tools.logger import Logger

from .map import run 
from .pseudonymise import pseudonymise

class PlatformNotSupported(Exception):
    pass

class UserNotSupported(Exception):
    pass

class DuplicateDataDetected(Exception):
    pass

class UnknownConfigurationSetting(Exception):
    pass

@click.group(help='Command group for running the full ETL of a dataset')
def etl():
    pass


def _load_config(config_file):
    stream = open(config_file) 
    config = yaml.safe_load(stream)
    return config

@click.group(help='Command group for ETL integration with bclink')
@click.option('--force','-f',help='Force running of this, useful for development purposes',is_flag=True)
@click.option('config_file','--config','--config-file',help='specify a yaml configuration file',required=True)
@click.pass_context
def bclink(ctx,force,config_file):

    if not force:
        #check the platform (i.e. should be centos)
        if os.name == 'nt':
            raise PlatformNotSupported(f"Not suported to run this on Windows")
        #check the username
        #for bclink, we need to run as bcos_srv to get access to all the datasettool2 etc. tools
        #and be able to connect with the postgres server without the need for a password
        user = os.environ.get("USER")
        if user != 'bcos_srv':
            raise UserNotSupported(f"{user} not supported! You must run this as user 'bcos_srv'")


    config = _load_config(config_file)
    
    rules = coconnect.tools.load_json(config['rules'])
    destination_tables = list(rules['cdm'].keys())
    
    bclink_settings = {}
    if 'bclink' in config:
        bclink_settings = config.pop('bclink')
    else:
        bclink_settings['tables'] = {x:x for x in destination_tables}
        bclink_settings['global_ids'] = 'global_ids'

    bclink_settings['tables'] = _get_table_map(bclink_settings['tables'],destination_tables)
    bclink_helpers = BCLinkHelpers(**bclink_settings)
    if ctx.obj is None:
        ctx.obj = dict()
    ctx.obj['bclink_helpers'] = bclink_helpers
    ctx.obj['rules'] = rules
    ctx.obj['data'] = config['data']
   
    log = 'coconnect.log'
    if 'log' in config.keys():
        log = config['log']
    ctx.obj['log'] = log
    ctx.obj['conf'] = config_file

    clean = False
    if 'clean' in config.keys():
        clean = config['clean']
    ctx.obj['clean'] = clean
    
    unknown_keys = list( set(config.keys()) - set(ctx.obj.keys()) )
    if len(unknown_keys) > 0 :
        raise UnknownConfigurationSetting(f'"{unknown_keys}" are not valid settings in the yaml file')

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
    data = ctx.obj['data']
    clean = ctx.obj['clean']
    rules = ctx.obj['rules']
    bclink_helpers = ctx.obj['bclink_helpers']
    config_file = ctx.obj['conf']
    
    bclink_helpers.print_summary()
    
    for item in data:
        input_folder = item['input']
        
        inputs = coconnect.tools.get_files(input_folder,type='csv')
        filtered_rules = coconnect.tools.remove_missing_sources_from_rules(rules,inputs)
 
        _execute(ctx,
                 data=item,
                 rules=filtered_rules,
                 clean=clean
             )
        clean = False
    
    logger.info(f"Finished!... Listening for changes to data in {config_file}")
    while True:
        
        conf = _load_config(config_file)
        if not (data == conf['data']):
            new_data = [obj for obj in conf['data'] if obj not in data]

            logger.info(f"New data found! {new_data}")

            for item in new_data:
                input_folder = item['input']
                inputs = coconnect.tools.get_files(input_folder,type='csv')
                filtered_rules = coconnect.tools.remove_missing_sources_from_rules(rules,inputs)
                _execute(ctx,
                         data=item,
                         rules=filtered_rules,
                         clean=False
                     )
            data += new_data
            logger.info(f"Finished!... Listening for changes to data in {config_file}")
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


    #clean outside of the following loop
    if clean:
        clean_tables = clean_folder = True
        if interactive:
            questions = [
                inquirer.Confirm('clean_tables',
                                 message=f"Clean all bclink tables? '"),
                inquirer.Confirm('clean_folder',
                                 message=f"Remove the folder '{output_folder}'")
            ]
            answers = inquirer.prompt(questions)
            if answers == None:
                os.kill(os.getpid(), signal.SIGINT)
            clean_tables = answers['clean_tables']
            clean_folder = answers['clean_folder']
   

        #if clean flag is true
        #clean the bclink tables
        if clean_tables:
            bclink_helpers.clean_tables()
        #remove the output folder
        if clean_folder and os.path.exists(output_folder) and os.path.isdir(output_folder):
            logger.info(f"removing old output_folder {output_folder}")
            shutil.rmtree(output_folder)
            
    i = 0
    while True:
        #find subfolders containing data dumps
        subfolders = coconnect.tools.get_subfolders(input_folder)
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
        for name,path in sorted(subfolders.items(),key=lambda x: os.path.getmtime(x[1])):
            output_folder_exists = os.path.exists(f"{output_folder}/{name}")
            
            inputs = coconnect.tools.get_files(path,type='csv')
            filtered_rules = coconnect.tools.remove_missing_sources_from_rules(rules,inputs)

            if output_folder_exists:
                output_tables = [
                    os.path.splitext(os.path.basename(x))[0]
                    for x in coconnect.tools.get_files(f"{output_folder}/{name}",type='tsv')
                ]
                
                expected_outputs = list(filtered_rules['cdm'].keys())
                to_process = list(set(expected_outputs) - set(output_tables))
                
                if len(to_process) == 0:
                    continue

                filtered_rules = coconnect.tools.filter_rules_by_destination_tables(filtered_rules,to_process)
               

            logger.debug(f"New data found!")
            logger.info(f"Creating a new task for processing {path}")
                
                            
            if len(inputs) == 0:
                logger.critical(f"Subfolder contains no .csv files!")
                continue
                    
            tables = list(filtered_rules['cdm'].keys())
            logger.debug(f'inputs: {inputs}')
            logger.info(f'cdm tables: {tables}')
                
            if interactive:
                choices = []
                location = f"{output_folder}/{name}"
               
                for table in tables:
                    source_tables = [
                        f"{location}/{x}"
                        for x in coconnect.tools.get_source_tables_from_rules(rules,table)
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
                    continue
                filtered_rules = coconnect.tools.filter_rules_by_destination_tables(filtered_rules,tables)
                logger.info(f'cdm tables: {tables}')
                    
  
            _data = copy.deepcopy(data)
            _data['input'] = inputs
            _data['output'] = f"{output_folder}/{name}"
        

            _execute(ctx,
                     data=_data,
                     rules=filtered_rules,
                     clean=False
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
        

@click.command(help='clean (delete all rows) in the bclink tables defined in the config file')
@click.option('--skip-local-folder',help="dont remove the local output folder",is_flag=True)
@click.pass_obj
def clean_tables(ctx,skip_local_folder,data=None):
    bclink_helpers = ctx['bclink_helpers']
    logger = Logger("clean_tables")

    if data is None:
        data = ctx['data']

    output_folder = data['output']
    if (not skip_local_folder) and os.path.exists(output_folder) and os.path.isdir(output_folder):
        logger.info(f"removing {output_folder}")
        shutil.rmtree(output_folder)
    
    bclink_helpers.clean_tables()
      

@click.command(help='delete data that has been inserted into bclink')
@click.pass_obj
def delete_data(ctx):
    bclink_helpers = ctx['bclink_helpers']
    logger = Logger("delete_data")
    logger.info("deleting data...")

    data = ctx['data']
    input_data = data['input']
    output_data = data['output']
    
    
    folders = coconnect.tools.get_subfolders(output_data)
    
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
        files = coconnect.tools.get_files(selected_folder,type='tsv')
                
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
    
    


@click.command(help='check the bclink tables')
@click.pass_obj
def check_tables(ctx):
    bclink_helpers = ctx['bclink_helpers']
    logger = Logger("check_tables")

    retval = {}
    logger.info("printing to see if tables exist")
    for bclink_table in bclink_helpers.table_map.values():
        retval[bclink_table] = bclink_helpers.check_table_exists(bclink_table)
    if bclink_helpers.global_ids:
        retval[bclink_helpers.global_ids] = bclink_helpers.check_table_exists(bclink_helpers.global_ids)

    logger.info(json.dumps(retval,indent=6))
    return retval


@click.command(help='crate new bclink tables')
@click.pass_context
def create_tables(ctx):
    logger = Logger("create_tables")
    exist = ctx.invoke(check_tables)

    tables_to_create = [
        bclink_table
        for bclink_table,exists in exist.items()
        if exists == False
    ]

    if len(tables_to_create) == 0:
        logger.info("All tables already exist!")
        return

    for table_name in tables_to_create:
        print (table_name)

    os.kill(os.getpid(), signal.SIGINT)
                



@click.command(help='Run the full ETL process for CO-CONNECT integrated with BCLink')
@click.option('--interactive','-i',help='run with interactive options - i.e. so user can confirm operations',is_flag=True)
@click.option('run_as_daemon','--daemon','-d',help='run the ETL as a daemon process',is_flag=True)
@click.pass_context
def execute(ctx,interactive,run_as_daemon):
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
        ctx.obj['interactive'] = interactive
        _process_data(ctx)


def _extract(ctx,data,rules,bclink_helpers):
    
    logger = Logger("extract")
    logger.info(f"starting extraction processes")

    do_pseudonymise=False
    if 'pseudonymise' in data:
        _pseudonymise = data['pseudonymise']
        
        chunksize = 1000
        output = "./pseudonymised_input_data/"
        if 'output' in _pseudonymise:
            output = _pseudonymise['output']

        if 'salt' not in _pseudonymise:
            raise Exception("To use pseudonymise a salt must be provided!")
        salt = _pseudonymise['salt']
        
        
        inputs = data['input']
        logger.info(f"Called do_pseudonymisation on input data {data} ")
        if not isinstance(rules,dict):
            rules = coconnect.tools.load_json(rules)
        person_id_map = coconnect.tools.get_person_ids(rules)
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
           
    indexer = bclink_helpers.get_indicies()
    return {
        'indexer':indexer,
        'data':data,
        'existing_global_ids':f_global_ids
    }

def _transform(ctx,rules,inputs,output_folder,indexer,existing_global_ids):
    logger = Logger("transform")
    logger.info("starting data transform processes")

    if isinstance(inputs,str):
        inputs = [inputs]

    ctx.invoke(run,
               rules=rules,
               inputs=inputs,
               output_folder=output_folder,
               indexing_conf=indexer,
               person_id_map=existing_global_ids
    ) 

def _load(output_folder,cdm_tables,global_ids,bclink_helpers):
    logger = Logger("load")
    logger.info("starting loading data processes")

    logger.info("starting loading global ids")
    if global_ids:
        bclink_helpers.load_global_ids(output_folder)
        
    logger.info("starting loading cdm tables")
    bclink_helpers.load_tables(output_folder,cdm_tables)

        
def _execute(ctx,rules=None,data=None,clean=None,bclink_helpers=None):
    
    if data == None:
        data = ctx.obj['data']
    if clean == None:
        clean = ctx.obj['clean']
    if rules == None:
        rules = ctx.obj['rules']
    if bclink_helpers == None:
        bclink_helpers = ctx.obj['bclink_helpers']
    
    interactive = ctx.obj['interactive']
         
    logger = Logger("execute")
    logger.info(f"Executing ETL...")
        
    if clean:
        logger.info(f"cleaning existing bclink tables")
        ctx.invoke(clean_tables,data=data)

    #call any extracting of data
    #----------------------------------
    extract_data= _extract(ctx,
                           data,
                           rules,
                           bclink_helpers
    ) 
    indexer = extract_data['indexer']
    existing_global_ids = extract_data['existing_global_ids']
    data = extract_data['data']
    #----------------------------------

    inputs = data['input']
    output_folder = data['output']
    
    #call transform
    #----------------------------------
    _transform(ctx,
               rules,
               inputs,
               output_folder,
               indexer,
               existing_global_ids
    )
    #----------------------------------       
    #remove this lookup file once done with it
    if existing_global_ids and os.path.exists(existing_global_ids):
        os.remove(existing_global_ids)

    cdm_tables = coconnect.tools.get_files(output_folder,type='tsv')
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
    _load(output_folder,
          cdm_tables,
          global_ids,
          bclink_helpers
    )


def _get_table_map(table_map,destination_tables):
    #if it's not a dict, and is a file, load the json
    if not isinstance(table_map,dict):
        table_map = coconnect.tools.load_json(table_map)

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

    _rules = coconnect.tools.load_json(rules)
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

                

bclink.add_command(clean_tables,'clean_tables')
bclink.add_command(delete_data,'delete_data')
bclink.add_command(check_tables,'check_tables')
bclink.add_command(create_tables,'create_tables')
bclink.add_command(execute,'execute')
etl.add_command(manual,'bclink-manual')
etl.add_command(bclink,'bclink')



