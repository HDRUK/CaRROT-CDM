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

schema = 'public'
omop_tables = [
    table
    for table in inspector.get_table_names(schema=schema)
]
omop_tables.sort()

print (json.dumps(omop_tables,indent=6))

 
selection = r'''
SELECT *
FROM public.concept
WHERE (concept_id=8507 OR concept_id=8532)
'''#8532
df = pd.read_sql(selection,ngin).drop(['valid_start_date','valid_end_date','invalid_reason'],axis=1)
print (df)

for table in filter(lambda tab: 'concept' in tab, omop_tables):
    selection = r'''
    SELECT *
    FROM public.%s
    LIMIT 10;
    '''%(table)
    print (table)
    df = pd.read_sql(selection,ngin)
    print (df.columns)
    print (df)
    print ()

exit(0)

def get_full_df(table,schema=schema):
    selection = r'''
    SELECT *
    FROM %s.%s
    '''%(schema,table)
    return pd.read_sql(selection,ngin)

print (get_full_df('person').columns.values)



#omop_fields = {
#    table: get_df(table).columns
#    for table in omop_tables
#}
#print (omop_fields)
