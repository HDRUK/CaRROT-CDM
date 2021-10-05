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
import pandas as pd

import coconnect
from coconnect.tools.bclink_helpers import BCLinkHelpers
from coconnect.tools.logger import Logger

from .map import run

class PlatformNotSupported(Exception):
    pass

class UserNotSupported(Exception):
    pass


@click.group(help='Command group for running the full ETL of a dataset')
def etl():
    pass


@click.group(help='Command group for ETL integration with bclink')
@click.option('--force','-f',help='Force running of this, useful for development purposes',is_flag=True)
def bclink(force):
    if force:
        return

    if os.name == 'nt':
        raise PlatformNotSupported(f"Not suported to run this on Windows")

    #check the username
    #for bclink, we need to run as bcos_srv to get access to all the datasettool2 etc. tools
    #and be able to connect with the postgres server without the need for a password
    user = os.environ.get("USER")
    if user != 'bcos_srv':
        raise UserNotSupported(f"{user} not supported! You must run this as user 'bcos_srv'")
   
    pass


def _process_data_from_list(ctx,data,rules,clean=False,bclink_config={},**kwargs):
    #loop over the list of data 
    for i,obj in enumerate(data):
        #get a new input folder
        input_folder = obj['input']
        #get a new output folder
        output_folder = obj['output']
        #invoke the running of the ETL
        
        ctx.invoke(manual,
                   rules=rules,
                   input_folder=input_folder,
                   output_folder=output_folder,
                   clean=clean if i==0 else False,
                   **bclink_config)

                    
def _from_yaml(ctx,logger,config):
    #print the configuration to the screen
    logger.info(json.dumps(config,indent=6))

    rules = data = None
    bclink_config = {}
    clean = False

    
    for key,obj in config.items():
        if key == 'rules':
            #get the location of the rules json file
            rules = obj
        elif key == 'bclink':
            bclink_config = {}
            for _key,_obj in obj.items():
                if _key == 'user' or _key == 'database':
                    bclink_config[_key] = _obj
                elif _key == 'gui user':
                    bclink_config['gui_user'] = _obj
                elif _key == 'tables':
                    bclink_config['table_map'] = _obj
                elif _key == 'dry-run':
                    bclink_config['dry_run'] = _obj
                else:
                    logger.warning(f"Unknown BCLink configuration {_key}:{_obj}")
            
        elif key == 'clean':
            #say if the tables should be cleaned
            #aka delete all rows before inserting new data
            clean = obj
        elif key == 'data':
            #load the configuration for i/o files
            data = obj
        elif key == 'log':
            #already handled
            pass
        else:
            logger.warning(f"Unknown key '{key}', skipping...")
        
    if rules == None:
        raise Exception("A rules file must be specified in the yaml configuration file... rules:<path to file>")
    if data == None:
        raise Exception("I/O data files/folders must be specified in the yaml configuration file...")

    if 'table_map' not in bclink_config:
        bclink_config['table_map'] =  {}
    for destination_table in coconnect.tools.load_json(rules)['cdm'].keys():
        if destination_table not in bclink_config['table_map']:
            bclink_config['table_map'][destination_table] = destination_table
     
    if isinstance(data,list):
        _process_data_from_list(ctx,
                                 data,
                                 rules,
                                 clean=clean,
                                 bclink_config=bclink_config
        )
        
    elif isinstance(data,dict) and 'watch' in data:
        #calculate the amount of time to wait before checking for changes
        watch = data['watch']
        tdelta = datetime.timedelta(**watch)

        #get the input folder to watch
        input_folder = data['input']
        #get the root output folder
        output_folder = data['output']
    
        bclink_helpers = BCLinkHelpers(**bclink_config)
        
        if clean:
            #if clean flag is true
            #remove the output folder
            if os.path.exists(output_folder) and os.path.isdir(output_folder):
                logger.info(f"removing old output_folder {output_folder}")
                shutil.rmtree(output_folder)
                bclink_helpers.clean_tables()
                                    
        while True:
            subfolders = { os.path.basename(f.path):f.path for f in os.scandir(input_folder) if f.is_dir() }
            logger.info(f"Found and checking {len(subfolders.values())} subfolders")
            if len(subfolders.values())> 0:
                logger.info(f"{list(subfolders.values())}")
            jobs = []
            for name,path in subfolders.items():
                if not os.path.exists(f"{output_folder}/{name}"):
                    logger.info(f"Creating a new task for processing {path} {name}")
                    jobs.append({
                        'input':path,
                        'output':f"{output_folder}/{name}" 
                    })
                else:
                    logger.warning(f"Already found a results folder for {path} "
                                   f"({output_folder}/{name}). "
                                   "Assuming this data has already been processed!")
            for job in jobs:
                ctx.invoke(manual,
                           rules=rules,
                           input_folder=job['input'],
                           output_folder=job['output'],
                           clean=False,
                           **bclink_config)
                
            logger.info(f"Now waiting {tdelta} before looking for new data files....")
            time.sleep(tdelta.total_seconds())
        else:
            raise Exception(f"No parameter 'watch' has been specified with {data}")

@click.command(help='check the bclink tables')
@click.argument('config_file')
@click.pass_context
def check_tables(ctx,config_file):
    logger = Logger("check_tables")
    stream = open(config_file) 
    config = yaml.safe_load(stream)
    table_map = config['bclink tables']
    
    for destination_table,bclink_table in table_map.items():
        obj = coconnect.cdm.get_cdm_class(destination_table)()
        fields = obj.fields
        dups = bclink_helpers.get_duplicates(bclink_table,fields)
        exit(0)
        



@click.command(help='Run with a yaml configuration file')
@click.option('run_as_daemon','--daemon','-d',help='run the ETL as a daemon process',is_flag=True)
@click.argument('config_file')
@click.pass_context
def from_yaml(ctx,config_file,run_as_daemon):
    logger = Logger("from_yaml")
    stream = open(config_file) 
    config = yaml.safe_load(stream)

    if run_as_daemon and daemon is None:
        raise ImportError(f"You are trying to run in daemon mode, "
                          "but the package 'daemon' hasn't been installed. "
                          "pip install python-daemon. \n"
                          "If you are running on a Windows machine, this package is not supported")

    if run_as_daemon and daemon is not None:
        stdout = 'coconnect.out'
        stderr = 'coconnect.log'
        if 'log' in config:
            stderr = config['log']

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
                _from_yaml(ctx,logger,config)
    else:
        _from_yaml(ctx,logger,config)


    
@click.command(help='Run manually')
@click.option('--rules','-r',help='location of the json rules file',required=True)
@click.option('--input-folder','-i',help='location of the input csv files',required=True)
@click.option('--output-folder','-o',help='location of the output results folder',required=True)
@click.option('--clean',help='clean all the BCLink tables first by removing all existing rows',is_flag=True)
@click.option('--table-map','-t',help='a look up json file that maps between the CDM table and the table name in BCLink',default=None)
@click.option('--gui-user',help='name of the bclink gui user',default='data')
@click.option('--user',help='name of the bclink user',default='bclink')
@click.option('--database',help='name of the bclink database',default='bclink')
@click.option('--dry-run',help='peform a dry-run of the bclink uplod',is_flag=True)
@click.pass_context
def manual(ctx,rules,input_folder,output_folder,clean,table_map,gui_user,user,database,dry_run):

    logger = Logger("ETL::BCLink")
    logger.info(f'Rules: {rules}')
    logger.info(f'Inputs: {input_folder}')
    logger.info(f'Output: {output_folder}')
    logger.info(f'Clean Tables: {clean}')
    config = coconnect.tools.load_json(rules)
    destination_tables = list(config['cdm'].keys())


    logger.info(f'Processing {destination_tables}')
    if table_map is not None:
        if not isinstance(table_map,dict):
            table_map = coconnect.tools.load_json(table_map)
        for table_name in destination_tables:
            if table_name not in table_map.keys():
                logger.error(f'{table_name} not specified in table map {table_map}')
                raise Exception(f"You must give the name of the bclink table for {table_name}")

        #drop any tables that are not mapped
        table_map = {k:v for k,v in table_map.items() if k in destination_tables}
    else:
        table_map = {x:x for x in destination_tables}

    logger.info(f'BCLink Table Map: {table_map}')
    bclink_helpers = BCLinkHelpers(gui_user=gui_user,
                                   user=user,
                                   database=database,
                                   table_map=table_map,
                                   dry_run=dry_run)
    
    if clean:
        bclink_helpers.clean_tables()
           
                  
    #calculate the indexing conf
    indexer = bclink_helpers.get_indicies()
    logger.info("Retrieved the index map:")
    logger.info(json.dumps(indexer,indent=6))

    if not indexer:
        indexer=None

    #run the transform
    ctx.invoke(run,rules=rules,inputs=[input_folder],output_folder=output_folder,indexing_conf=indexer)

    #submit jobs to load
    bclink_helpers.load_tables(output_folder)
   
    for table_name in table_map.values():
        logger.info(f"Checking jobs submitted for {table_name}")
        
        stats = bclink_helpers.get_table_jobs(table_name)
        if stats is None:
            #is a dry run, just test this
            bclink_helpers.check_logs(0)
            return
        logger.info(stats)

        job_id = stats.iloc[0]['JOB']
        while True:
            logger.info(f"Getting log for {table_name} id={job_id}")
            success = bclink_helpers.check_logs(job_id)
            if success:
                break
            else:
                logger.warning(f"Didn't find the log for {table_name} id={job_id} yet, job still running.")
                time.sleep(1)
    

        

bclink.add_command(check_tables,'check_tables')
bclink.add_command(from_yaml,'from_yaml')
bclink.add_command(manual,'manual')
etl.add_command(bclink,'bclink')



