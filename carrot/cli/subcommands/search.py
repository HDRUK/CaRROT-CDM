import os
import click
import json
import carrot
import pandas as pd
import numpy as np
import requests
from dotenv import dotenv_values
    
class MissingToken(Exception):
    pass


@click.group(help='Commands for search/help with mapping')
def search():
    pass



@click.group(help='Commands to get data from the CCOM OMOP database.')
@click.pass_context
def omop(ctx):
    from sqlalchemy import create_engine, inspect
    from sqlalchemy.schema import CreateSchema
    from sqlalchemy_utils import database_exists, create_database

    config = dotenv_values(".env")
    
    host = config['carrot_DB_HOST']
    port = config['carrot_DB_PORT']
    dbname = config['carrot_DB_NAME']
    user = config['carrot_DB_USER']
    password = config['carrot_DB_PASSWORD']
    
    connection_str = f'postgresql://{user}:{password}@{host}/{dbname}'
    
    psql_engine = create_engine(connection_str)

    #insp  = inspect(psql_engine)
    #existing_tables = insp.get_table_names()

    ctx.obj = psql_engine
    

@click.command(help='omop code')
@click.option("--concept-id",help='omop code')
@click.option("--concept-name",help='omop code')
@click.option("--standard",is_flag=True)
@click.pass_obj
def concept(psql_engine,concept_id,concept_name,standard):

    if concept_id:
        query = f"SELECT * FROM concept WHERE concept_id='{concept_id}'"
    elif concept_name:
        #query = f"SELECT * FROM concept WHERE concept_name LIKE '%{concept_name}%'"
        query = f"SELECT * FROM concept WHERE concept_name ILIKE '%%{concept_name}%%'"
    else:
        click.echo("Error... you must used --concept-id or --concept-name")
        return

    if standard:
        query += " and standard_concept='S'"
        
    df = pd.read_sql(query,psql_engine).astype(str)#.squeeze(axis=0)
    #print (df)#json.dumps(df.to_dict(),indent=6))
    #for series in df:
    #    print (series)
    data = df.to_dict(orient='records')
    print (json.dumps(data,indent=6))



omop.add_command(concept,"concept")
search.add_command(omop,"omop")
#search.add_command(mongo,"mongo")


