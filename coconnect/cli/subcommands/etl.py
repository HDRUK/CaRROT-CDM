import click
import os
import coconnect
from coconnect.tools.logger import Logger
from .map import run
import yaml
import subprocess
from subprocess import Popen, PIPE


def run_cmd(cmd,logger):
    session = subprocess.Popen(cmd, stdout=PIPE, stderr=PIPE)
    stdout, stderr = (x.decode("utf-8") for x in session.communicate())
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

@click.group(help='Commands run ETL of a dataset for bclink')
def bclink():
    pass

@click.command(help='Run with a yaml configuration file')
@click.argument('config_file')
@click.option('--clean',help='clean all the BCLink tables first by removing all existing rows',is_flag=True)
@click.pass_context
def from_yaml(ctx,config_file,clean):
    with open(config_file) as stream:
        data = yaml.safe_load(stream)

        rules = data['rules']
        input_folder = data['input']
        output_folder = data['output']
        table_map = data['bclink tables']

        ctx.invoke(manual,
                   rules=rules,
                   input_folder=input_folder,
                   output_folder=output_folder,
                   table_map=table_map,
                   clean=clean)
    
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

    #run the load
    for table,table_name in table_map.items():
        data_file = f"{output_folder}/{table}.tsv"
        if not os.path.exists(data_file):
            logger.error(f"{data_file} does not exist, so cannot load it into bclink")
            continue
        cmd = ['load_tables.sh',data_file,table_name]
        run_cmd(cmd,logger)
    

bclink.add_command(from_yaml,'from_yaml')
bclink.add_command(manual,'manual')
etl.add_command(bclink,'bclink')



