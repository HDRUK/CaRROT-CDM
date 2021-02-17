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

ngin = sql.create_engine(f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')
inspector = sql.inspect(ngin)

[ print(table) for table in inspector.get_table_names(schema='public')]


def get_df(table):
    selection = r'''
    SELECT *
    FROM public.%s
    '''%(table)
    return pd.read_sql(selection,ngin)


scan_report_values = get_df('mapping_scanreportvalue')[['id','conceptID','scan_report_field_id']]
scan_report_values = scan_report_values.rename({'id':'value_id'},axis=1)


scan_reports = get_df('mapping_scanreporttable').set_index('id')
#scan_reports_aux = get_df('mapping_scanreport')[['id','dataset']]

scan_report = scan_reports[['scan_report_id','name']].rename({'name':'Source Table'},axis=1)

scan_report_fields = get_df('mapping_scanreportfield').set_index('scan_report_table_id')[['id','name']]
scan_report_fields = scan_report_fields.rename({'name':'Source Field'},axis=1)


scan_report = scan_report.join(scan_report_fields).set_index('id')


mapping_rules = get_df('mapping_mappingrule')[['scan_report_field_id','id','omop_field_id']]


mapping_rules = mapping_rules.merge(scan_report,left_index=True,right_index=True).sort_values('Source Field')
mapping_rules = mapping_rules.rename({'id':'rule_id'},axis=1)


mapping_omop = get_df('mapping_omopfield')
mapping_omoptable = get_df('mapping_omoptable')[['id','table']]

mapping_rules = mapping_rules.merge(mapping_omop,left_on='omop_field_id',right_on='id')
#mapping_rules = mapping_rules.drop(['omop_field_id','id', 'created_at', 'updated_at'],axis=1)
mapping_rules = mapping_rules.drop(['id','created_at', 'updated_at'],axis=1)


#mapping_rules = mapping_rules[['rule_id','scan_report_id','Source Table', 'Source Field', 'field', 'table_id']]

mapping_rules = mapping_rules.merge(mapping_omoptable,left_on='table_id',right_on='id')
mapping_rules = mapping_rules.drop(['table_id','id'],axis=1)

#mapping_rules = mapping_rules[['rule_id', 'scan_report_id','Source Table', 'Source Field', 'field', 'table']]
mapping_rules = mapping_rules.rename({'field':'Destination Field','table':'Destination Table'},axis=1)


scan_report_values = scan_report_values[scan_report_values['conceptID']>=0]

scan_report_values = scan_report_values.groupby(['scan_report_field_id'])[['conceptID']].count().reset_index()
scan_report_values = scan_report_values.rename({'conceptID':'conceptIDCount'},axis=1)

mapping_rules = mapping_rules.set_index('scan_report_field_id').join(scan_report_values.set_index('scan_report_field_id'))
mapping_rules['term_mapping'] = mapping_rules['conceptIDCount'].notnull().replace({True: 'y', False: 'n'})

mapping_rules = mapping_rules.drop('conceptIDCount',axis=1)

print (mapping_rules)

#print (scan_report_values)
#print (test)
#print (test.merge(scan_report_values,left_on='omop_field_id',right_on='scan_report_field_id'))
