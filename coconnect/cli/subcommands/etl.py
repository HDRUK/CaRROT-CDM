import click
import os
import shutil
import io
import time
import datetime
import coconnect
import coconnect.tools.bclink_helpers as bclink_helpers
from coconnect.tools.logger import Logger
from .map import run
import pandas as pd
import yaml
import json
import subprocess
from subprocess import Popen, PIPE


def run_cmd(cmd,logger=None):
    session = subprocess.Popen(cmd, stdout=PIPE, stderr=PIPE)
    stdout, stderr = (x.decode("utf-8") for x in session.communicate())

    if logger != None:
        for msg in stdout.split('\n'):
            if msg != '':
                logger.info(msg)
        for msg in stderr.split('\n'):
            if msg != '':
                logger.warning(msg)
    
    return stdout,stderr

@click.group(help='Commands run ETL of a dataset')
def etl():
    pass

class UserNotSupported(Exception):
    pass

@click.group(help='Commands run ETL of a dataset for bclink')
def bclink():
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
        destination_tables = list(rules['cdm'].keys())
        if 'bclink tables' in data:
            table_map = data['bclink tables']
        else:
            table_map = {x:x for x in destination_tables}

        missing = set(destination_tables) - set(table_map.keys())
        if len(missing)>0:
            logger.error(f"{missing} are missing from the bclink table map")

        #index_map = bclink_helpers.get_indicies(table_map)
        #logger.info("Retrieved the index map:")
        #logger.info(json.dumps(index_map,indent=6))

        for table in table_map.values():
            stdout,stderr = bclink_helpers.clean_table(table)
       
        msgs = bclink_helpers.load_tables(table_map,"results/001")
        print (msgs)

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



@click.command(help='Run with a yaml configuration file')
@click.argument('config_file')
@click.pass_context
def from_yaml(ctx,config_file):
    logger = Logger("from_yaml")
    with open(config_file) as stream:
        config = yaml.safe_load(stream)

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

            watch = data['watch']
            tdelta = datetime.timedelta(**watch)
            input_folder = data['input']
            output_folder = data['output']

            if clean:
                if os.path.exists(output_folder) and os.path.isdir(output_folder):
                    shutil.rmtree(output_folder)

                for table in table_map.values():
                    logger.info(f"cleaning table {table}")
                    stdout,stderr = bclink_helpers.clean_table(table)
                    for msg in stdout.splitlines():
                        logger.info(msg)
                    for msg in stderr.splitlines():
                        logger.warning(msg)
                    


            logger.info(f"Watching {input_folder} every {tdelta}")

            while True:
                subfolders = { os.path.basename(f.path):f.path for f in os.scandir(input_folder) if f.is_dir() }
                logger.info(f"Found and checking subfolders {list(subfolders.values())}")
                
                jobs = []
                for name,path in subfolders.items():
                    if not os.path.exists(f"{output_folder}/{name}"):
                        logger.info(f"Creating a new task for processing {path} {name}")
                        jobs.append({
                            'input':path,
                            'output':f"{output_folder}/{name}" 
                        })
                    else:
                        logger.warning(f"Already found a results folder for {path} ({output_folder}/{name}). Assuming this data has already been processed!")

                
                
                for job in jobs:
                    ctx.invoke(manual,
                               rules=rules,
                               input_folder=job['input'],
                               output_folder=job['output'],
                               table_map=table_map,
                               clean=False)

                logger.info(f"Now waiting {tdelta}....")
                time.sleep(tdelta.total_seconds())

    
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

    inv_table_map = {v: k for k, v in table_map.items()}
    
    if clean:
        cmd = ['drop_tables.sh'] + list(table_map.values())
        run_cmd(cmd,logger)

    #calculate the indexing conf
    cmd = ['create_indexer.sh'] + list(table_map.values())
    stdout,_ = run_cmd(cmd,logger)
    indexer = {}
    for line in stdout.split('\n'):
        if line == '': continue
        table,index = line.split(',')
        indexer[inv_table_map[table]]=int(index)

    if not indexer:
        indexer=None

    #run the transform
    ctx.invoke(run,rules=rules,inputs=[input_folder],output_folder=output_folder,indexing_conf=indexer)

    #submit jobs to load
    for table,table_name in table_map.items():
        data_file = f"{output_folder}/{table}.tsv"
        if not os.path.exists(data_file):
            logger.error(f"{data_file} does not exist, so cannot load it into bclink")
            continue
        cmd = ['load_tables.sh',data_file,table_name]#,job_name]
        run_cmd(cmd,logger)

    for table,table_name in table_map.items():
        cmd = f'datasettool2 list-updates --dataset={table_name} --user=data --database=bclink'.split(' ')
        queue_status,_ = run_cmd(cmd)
        info = pd.read_csv(io.StringIO(queue_status),
                           sep='\t',
                           usecols=['BATCH',
                                    'UPDDATE',
                                    'UPD_COMPLETION_DATE',
                                    'JOB',
                                    'STATUS',
                                    'ACTION']).head(5)
        logger.info(info)

        job_id = info.iloc[0]['JOB']
        cover = f'/data/var/lib/bcos/download/data/logs/{table_name}/cover.{job_id}'
        while True:
            if os.path.exists(cover):
                break
            time.sleep(1)

        cmd = ['cat',cover]
        run_cmd(cmd,logger)
        

bclink.add_command(check_yaml,'check_yaml')
bclink.add_command(from_yaml,'from_yaml')
bclink.add_command(manual,'manual')
etl.add_command(bclink,'bclink')



