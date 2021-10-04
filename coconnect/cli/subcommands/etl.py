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
import coconnect.tools.bclink_helpers as bclink_helpers
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
def bclink():
    if os.name == 'nt':
        raise PlatformNotSupported(f"Not suported to run this on Windows")


    #check the username
    #for bclink, we need to run as bcos_srv to get access to all the datasettool2 etc. tools
    #and be able to connect with the postgres server without the need for a password
    user = os.environ.get("USER")
    if user != 'bcos_srv':
        raise UserNotSupported(f"{user} not supported! You must run this as user 'bcos_srv'")

   
    pass

@click.command(help='Check the parameters in the yaml configuration file')
@click.argument('config_file')
@click.pass_context
def check_yaml(ctx,config_file):
    logger = Logger("check_yaml")
    with open(config_file) as stream:
        data = yaml.safe_load(stream)
        rules = coconnect.tools.load_json(data['rules'])

        logger.info(json.dumps(data,indent=6))

        destination_tables = list(rules['cdm'].keys())
        if 'bclink tables' in data:
            table_map = data['bclink tables']
        else:
            table_map = {x:x for x in destination_tables}

        missing = set(destination_tables) - set(table_map.keys())
        if len(missing)>0:
            logger.error(f"{missing} are missing from the bclink table map")

        index_map = bclink_helpers.get_indicies(table_map,dry_run=True)
        for line in index_map.values():
            logger.info(f"Execute: {line}")

        index_map = bclink_helpers.get_indicies(table_map,dry_run=False)
        logger.info("Will start indices from:")
        logger.info(json.dumps(index_map,indent=6))

        if data['clean']:
            logger.info("Cleaning of tables on start-up is turned on...")
            for table in table_map.values():
                stdout,stderr = bclink_helpers.clean_table(table,dry_run=True)
                for msg in stdout.splitlines():
                    logger.info(f"Execute: {msg}")
       
        #msgs = bclink_helpers.load_tables(table_map,"results/001")
        #print (msgs)
 
        #for table in table_map.values():
        #    stats = bclink_helpers.get_table_jobs(table)
        #    print (stats)

        #for table in table_map.values():
        #    stdout,stderr = bclink_helpers.clean_table(table)
        #    for msg in stderr.splitlines():
        #        logger.info(msg)
        #index_map = bclink_helpers.get_indicies(table_map)
        #logger.info("Retrieved the index map:")
        #logger.info(json.dumps(index_map,indent=6))



def _from_yaml(ctx,logger,config):
   
    logger.info(json.dumps(config,indent=6))
    
    rules = config['rules']
     
    if 'bclink tables' in config:
        table_map = config['bclink tables']
    else:
        table_map = None

    if 'clean' in config:
        clean = config['clean']
    else:
        clean = False

    data = config['data']
    
    if isinstance(data,list):
        for i,obj in enumerate(data):
            input_folder = obj['input']
            output_folder = obj['output']
            ctx.invoke(manual,
                       rules=rules,
                       input_folder=input_folder,
                       output_folder=output_folder,
                       table_map=table_map,
                       clean=clean if i==0 else False)
    else:

        if clean:
            if os.path.exists(output_folder) and os.path.isdir(output_folder):
                logger.info(f"removing old output_folder {output_folder}")
                shutil.rmtree(output_folder)
            for table in table_map.values():
                logger.info(f"cleaning table {table}")
                stdout,stderr = bclink_helpers.clean_table(table)
                for msg in stdout.splitlines():
                    logger.info(msg)
                for msg in stderr.splitlines():
                    logger.warning(msg)
                            
        watch = None
        if 'watch' in data:
            watch = data['watch']

        if watch is not None:
            tdelta = datetime.timedelta(**watch)
            input_folder = data['input']
            output_folder = data['output']
            logger.info(f"Watching {input_folder} every {tdelta}")
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
                               table_map=table_map,
                               clean=False)
                
                logger.info(f"Now waiting {tdelta} before looking for new data files....")
                time.sleep(tdelta.total_seconds())
        else:
            print ('no watch!')

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
@click.pass_context
def manual(ctx,rules,input_folder,output_folder,clean,table_map):

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
    else:
        table_map = {x:x for x in destination_tables}

    logger.info(f'BCLink Table Map: {table_map}')
    
    if clean:
        for table in table_map.values():
            stdout,stderr = bclink_helpers.clean_table(table)
            for msg in stdout.splitlines():
                logger.info(msg)
            for msg in stderr.splitlines():
                logger.warning(msg)
                
    #calculate the indexing conf
    indexer = bclink_helpers.get_indicies(table_map)
    logger.info("Retrieved the index map:")
    logger.info(json.dumps(indexer,indent=6))

    if not indexer:
        indexer=None

    #run the transform
    ctx.invoke(run,rules=rules,inputs=[input_folder],output_folder=output_folder,indexing_conf=indexer)

    #submit jobs to load
    msgs = bclink_helpers.load_tables(table_map,output_folder)
    for msg in msgs:
        logger.info(f"submitted job to bclink queue: {msg}")

    for table_name in table_map.values():
        logger.info(f"Checking jobs submitted for {table_name}")
        
        stats = bclink_helpers.get_table_jobs(table_name)
        logger.info(stats)

        job_id = stats.iloc[0]['JOB']
        while True:
            logger.info(f"Getting log for {table_name} id={job_id}")
            log_msgs = bclink_helpers.check_logs(job_id)
            if log_msgs is not None:
                for msg in log_msgs:
                    logger.info(msg)
                break
            else:
                logger.warning(f"Didn't find the log for {table_name} id={job_id} yet")
                time.sleep(1)
    

        

bclink.add_command(check_yaml,'check_yaml')
bclink.add_command(from_yaml,'from_yaml')
bclink.add_command(manual,'manual')
etl.add_command(bclink,'bclink')



