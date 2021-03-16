import json
import os
import sqlalchemy as sql
import pandas as pd

from dotenv import load_dotenv
load_dotenv()

db_name = os.environ['POSTGRES_DB']
db_user = os.environ['POSTGRES_USER']
db_password = os.environ['POSTGRES_PASSWORD']
db_host = os.environ['POSTGRES_HOST']
db_port = int(os.environ['POSTGRES_PORT'])


#need special format for azure
#https://github.com/MicrosoftDocs/azure-docs/issues/6371#issuecomment-376997025
con_str =f'postgresql://{db_user}%40{db_host}:{db_password}@{db_host}:{db_port}/{db_name}'

ngin = sql.create_engine(con_str)
inspector = sql.inspect(ngin)

#schema = 'information_schema'
#info_tables = [
#    table
#    for table in inspector.get_table_names(schema=schema)
#]

schema = 'public'
omop_tables = [
    table
    for table in inspector.get_table_names(schema=schema)
]
omop_tables.sort()

print (json.dumps(omop_tables,indent=6))

def get_df(table,schema=schema):
    selection = r'''
    SELECT *
    FROM %s.%s
    '''%(schema,table)
    return pd.read_sql(selection,ngin)

print (get_df('person').columns.values)

#omop_fields = {
#    table: get_df(table).columns
#    for table in omop_tables
#}
#print (omop_fields)
