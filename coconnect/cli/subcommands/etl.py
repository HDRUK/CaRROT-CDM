import click
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


    stream = open(config_file) 
    config = yaml.safe_load(stream)
    
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

    clean = False
    if 'clean' in config.keys():
        clean = config['clean']
    ctx.obj['clean'] = clean
    
    unknown_keys = list( set(config.keys()) - set(ctx.obj.keys()) )
    if len(unknown_keys) > 0 :
        raise UnknownConfigurationSetting(f'"{unknown_keys}" are not valid settings in the yaml file')

def _process_data(ctx):
    logger = Logger("_process_data")
    data = ctx.obj['data']
    clean = ctx.obj['clean']
    
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
        #if clean flag is true
        #clean the bclink tables
        bclink_helpers.clean_tables()
        #remove the output folder
        if os.path.exists(output_folder) and os.path.isdir(output_folder):
            logger.info(f"removing old output_folder {output_folder}")
            shutil.rmtree(output_folder)
            
    i = 0
    while True:
        subfolders = { os.path.basename(f.path):f.path for f in os.scandir(input_folder) if f.is_dir() }
        logger.debug(f"Found and checking {len(subfolders.values())} subfolders")
        logger.debug(list(subfolders.values()))
        if len(subfolders.values())> 0:
            logger.debug(f"{list(subfolders.values())}")
            
        jobs = []
        for name,path in subfolders.items():
            if not os.path.exists(f"{output_folder}/{name}"):
                logger.info(f"New folder found! Creating a new task for processing {path} {name}")
                
                inputs = [x.path for x in os.scandir(path) if x.path.endswith('.csv')]
                if len(inputs) == 0:
                    logger.critical(f"New subfolder contains no .csv files!")
                    continue
                    
                _data = copy.deepcopy(data)
                _data['input'] = inputs
                _data['output'] = f"{output_folder}/{name}"
                jobs.append(_data)
            else:
                logger.debug(f"Already found a results folder for {path} "
                             f"({output_folder}/{name}). "
                             "Assuming this data has already been processed!")
        for job in jobs:
          
            _execute(ctx,
                     data=job,
                     clean=False
            )
            
        if tdelta is None:
            break
                
        if len(jobs)>0 or i==0:
            logger.info(f"Refreshing {input_folder} every {tdelta} to look for new subfolders....")
            if len(subfolders.values()) == 0:
                logger.warning("No subfolders for data dumps yet found...")

        i+=1
        time.sleep(tdelta.total_seconds())
        

@click.command(help='clean (delete all rows) in the bclink tables defined in the config file')
@click.pass_obj
def clean_tables(ctx):
    bclink_helpers = ctx['bclink_helpers']
    logger = Logger("clean_tables")
    bclink_helpers.clean_tables()


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

    exit(0)
        

    stream = open(config_file) 
    config = yaml.safe_load(stream)
    print (config)
    table_map = config['bclink']['tables']
    
    for destination_table,bclink_table in table_map.items():
        obj = coconnect.cdm.get_cdm_class(destination_table)()
        fields = obj.fields
        print (destination_table)
        #dups = bclink_helpers.get_duplicates(bclink_table,fields)
        #exit(0)
        



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
        stderr = ctx['log']
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
        
    indexer = bclink_helpers.get_indicies()
    return {'indexer':indexer,'data':data}

def _transform(ctx,rules,inputs,output_folder,indexer):
    logger = Logger("transform")
    logger.info("starting data transform processes")
    ctx.invoke(run,
               rules=rules,
               inputs=inputs,
               output_folder=output_folder,
               indexing_conf=indexer
    ) 

def _load(output_folder,bclink_helpers):
    logger = Logger("load")

    logger.info("starting loading data processes")

    logger.info("First, check for duplicate person_ids")
    #check to see if the person ids have already been loaded
    if not bclink_helpers.check_global_ids(output_folder):
        logger.error("Failed in check of global IDs, will not load tables")
        logger.warning("Will now clean-up results folder")
        #remove the output folder
        if os.path.exists(output_folder) and os.path.isdir(output_folder):
            logger.info(f"removing {output_folder}")
            shutil.rmtree(output_folder)
        raise DuplicateDataDetected("You are trying to load person_ids that have already been inserted"
                                    f"... duplicated person table has been detected in {output_folder}")
    else:
        logger.info("starting loading global ids")
        bclink_helpers.load_global_ids(output_folder)
        
        logger.info("starting loading cdm tables")
        bclink_helpers.load_tables(output_folder)

        
def _execute(ctx,rules=None,data=None,clean=None,bclink_helpers=None):
    
    if data == None:
        data = ctx.obj['data']
    if clean == None:
        clean = ctx.obj['clean']
    if rules == None:
        rules = ctx.obj['rules']
    if bclink_helpers == None:
        bclink_helpers = ctx.obj['bclink_helpers']
        
    logger = Logger("execute")
    logger.info(f"Executing ETL...")
        
    if clean:
        logger.info(f"cleaning existing bclink tables")
        bclink_helpers.clean_tables()

    #call any extracting of data
    #----------------------------------
    extract_data= _extract(ctx,
                           data,
                           rules,
                           bclink_helpers
    ) 
    indexer = extract_data['indexer']
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
               indexer
    )
    #----------------------------------       

    #call load
    #----------------------------------        
    _load(output_folder,
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
bclink.add_command(check_tables,'check_tables')
bclink.add_command(create_tables,'create_tables')
bclink.add_command(execute,'execute')
etl.add_command(manual,'bclink-manual')
etl.add_command(bclink,'bclink')



