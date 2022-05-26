import os
import click
import json
import yaml
import carrot
import pandas as pd
import numpy as np
import requests
import secrets
import random
import datetime
import time

class MissingToken(Exception):
    pass


@click.group(help='Commands to generate helpful files.')
def generate():
    pass

@click.group(help='Commands to generate synthetic data.')
def synthetic():
    pass

@click.command(help="generate synthetic data from a ScanReport ID from CCOM")
@click.option("-i","--report-id",help="ScanReport ID on the website",required=True,type=int)
@click.option("-n","--number-of-events",help="number of rows to generate",required=True,type=int)
@click.option("-o","--output-directory",help="folder to save the synthetic data to",required=True,type=str)
@click.option("--fill-column-with-values",help="select columns to fill values for",multiple=True,type=str)
@click.option("-t","--token",help="specify the carrot_token for accessing the CCOM website",type=str,default=None)
@click.option("--get-json",help="also download the json",is_flag=True)
@click.option("-u","--url",help="url endpoint for the CCOM website to ping",
              type=str,
              default="https://ccom.azurewebsites.net")
def ccom(report_id,number_of_events,output_directory,
         fill_column_with_values,token,get_json,
         url):

    fill_column_with_values = list(fill_column_with_values)
    
    token = os.environ.get("carrot_TOKEN") or token
    if token == None:
        raise MissingToken("you must use the option --token or set the environment variable carrot_TOKEN to be able to use this functionality. I.e  export carrot_TOKEN=12345678 ")

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.119 Safari/537.36",
        "Content-type": "application/json",
        "charset": "utf-8",
        "Authorization": f"Token {token}"
    }
    
    if get_json:
        response = requests.get(
            f"{url}/api/json/?id={report_id}",
            headers=headers
        )
        print (json.dumps(response.json()[0],indent=6))
        fname = response.json()[0]['metadata']['dataset']
        with open(f'{fname}.json', 'w') as outfile:
            print ('saving',fname)
            json.dump(response.json()[0],outfile,indent=6)
            
    
    response = requests.get(
        f"{url}/api/scanreporttablesfilter/?scan_report={report_id}",
        headers=headers
    )
    if response.status_code != 200:
        print ('failed to get a response')
        print (response.json())
        exit(0)
    else:
        print (json.dumps(response.json(),indent=6))
        
    for table in response.json():
        name = table['name']
        _id = table['id']
        
        #get which is the person_id and automatically fill this with incrementing values
        #so they are not all NaN in the synthetic data (because of List Truncated...)
        person_id = table['person_id']
        if person_id == None:
            continue
        
        _url = f"{url}/api/scanreportfieldsfilter/?id={person_id}&fields=name"

        person_id = requests.get(
            _url, headers=headers,
            allow_redirects=True,
        ).json()[0]['name'].lstrip('\ufeff')
        
                
        _url = f"{url}/api/scanreportvaluesfilterscanreporttable/?scan_report_table={_id}&fields=value,frequency,scan_report_field"
        response = requests.get(
            _url, headers=headers,
            allow_redirects=True,
        )

                
        df = pd.DataFrame.from_records(response.json()).set_index('scan_report_field')        
        _url = f"{url}/api/scanreportfieldsfilter/?scan_report_table={_id}&fields=id,name"
        response = requests.get(
            _url, headers=headers,
            allow_redirects=True,
        )

        
        res = json.loads(response.content.decode('utf-8'))
        id_to_col_name = {
            field['id']:field['name'].lstrip('\ufeff')
            for field in res
        }
        
        df.index = df.index.map(id_to_col_name)

        
        df_synthetic = {}
        
        for col_name in df.index.unique():
            if col_name == '': continue
            
            _df = df.loc[[col_name]]
            _df['value'].replace('',np.nan,inplace=True)

            _df = _df.dropna()
            
            
            if len(_df) > number_of_events:
                _df = _df.sample(frac=1)[:number_of_events]

            
            frequency = _df['frequency']
            total = frequency.sum()

            
            if total > 0 :
                frequency = number_of_events*frequency / total
                frequency = frequency.astype(int)
            else:
                frequency = number_of_events
                
            values = _df['value'].repeat(frequency)\
                                 .sample(frac=1)\
                                 .reset_index(drop=True)

            values.name = col_name
            df_synthetic[col_name] = values

        df_synthetic = pd.concat(df_synthetic.values(),axis=1)
        
        
        for col_name in fill_column_with_values:
            if col_name in df_synthetic.columns:
                df_synthetic[col_name] = df_synthetic[col_name].reset_index()['index']
                
        
        if not os.path.isdir(output_directory):
            os.makedirs(output_directory)
        fname = f"{output_directory}/{name}"

        df_synthetic.index = 'pk'+df_synthetic.index.astype(str)
        df_synthetic.rename_axis(person_id,inplace=True)
        #df_synthetic.set_index(df_synthetic.columns[0],inplace=True)
        print (df_synthetic)
        df_synthetic.to_csv(fname)
        print (f"created {fname} with {number_of_events} events")
        
    
@click.command(help="generate synthetic data from a ScanReport xlsx file")
@click.argument("report")
@click.option("-n","--number-of-events",help="number of rows to generate",required=True,type=int)
@click.option("-o","--output-directory",help="folder to save the synthetic data to",required=True,type=str)
@click.option("--fill-column-with-values",help="select columns to fill values for",multiple=True,type=str)
def xlsx(report,number_of_events,output_directory,fill_column_with_values):
    dfs = pd.read_excel(report,sheet_name=None)
    sheets_to_process = list(dfs.keys())[2:-1]

    for name in sheets_to_process:
        df = dfs[name]
        columns_to_make = [
            x
            for x in df.columns
            if 'Frequency' not in x and 'Unnamed' not in x
        ]

        df_synthetic = {}
        for col_name in columns_to_make:
            i_col = df.columns.get_loc(col_name)
            df_stats = df.iloc[:,[i_col,i_col+1]].dropna()

            if not df_stats.empty:
                frequency = df_stats.iloc[:,1]
                frequency = number_of_events*frequency / frequency.sum()
                frequency = frequency.astype(int)

                values = df_stats.loc[df_stats.index.repeat(frequency)]\
                                 .iloc[:,0]\
                                 .sample(frac=1)\
                                 .reset_index(drop=True)
                df_synthetic[col_name] = values
            else:
                df_synthetic[col_name] = df_stats.iloc[:,0]
                
        df_synthetic = pd.concat(df_synthetic.values(),axis=1)

        for col_name in fill_column_with_values:
            if col_name in df_synthetic.columns:
                df_synthetic[col_name] = df_synthetic[col_name].reset_index()['index']
                
        
        if not os.path.isdir(output_directory):
            os.makedirs(output_directory)
        fname = f"{output_directory}/{name}"
        #ext = fname[-3:]
        df_synthetic.set_index(df_synthetic.columns[0],inplace=True)
        print (df_synthetic)
        df_synthetic.to_csv(fname)
        print (f"created {fname} with {number_of_events} events")


synthetic.add_command(xlsx,"from_xlsx")
synthetic.add_command(ccom,"from_ccom")

@click.command(help="generate a synthetic CDM from a yaml file")
@click.argument("config")
def synthetic_cdm(config):
    with open(config, 'r') as stream:
        config = yaml.safe_load(stream)

    dfs = {}
    order = sorted(config.items(), key=lambda x: x[0] != 'person')
    for destination_table_name,destination_table in order:
        n = destination_table['n']
        obj = carrot.cdm.get_cdm_class(destination_table_name)()
        columns = destination_table['columns']
        for column,spec in columns.items():
            if 'range' in spec:
                _range = spec['range']
                _min = 0
                if 'min' in _range:
                    _min = _range['min']
                _max = _min + n
                x = range(_min,_max)
                x = pd.Series(x)
            elif 'random' in spec:
                _map = spec['random']
                _df = pd.Series(_map).to_frame()
                x = _df.index.to_series().repeat(_df[0]*n).sample(frac=1).reset_index(drop=True)
            elif 'map' in spec:
                _map = spec['map']
                col_to_map,_map = list(_map.items())[0]
                x = obj[col_to_map].series.map(_map)
            elif 'gaus' in spec:
                _range = spec['gaus']
                mu = _range['mean']
                sigma = (_range['max'] - _range['min']).total_seconds()/5
                mu = time.mktime(mu.timetuple())
                x = [datetime.date.fromtimestamp(x) for x in np.random.normal(mu,sigma,n)]
                x = pd.Series(x)
            elif 'person' in spec:
                x = dfs['person']['person_id'].sample(n,replace=True).sort_values().reset_index(drop=True)
            else:
                print (spec)
                exit(0)

            x.name = column
            print (x)
            obj[column].series = x

        df = obj.get_df()
        print (df.dropna(axis=1))
        dfs[destination_table_name] = df
    
synthetic.add_command(synthetic_cdm,"cdm")


generate.add_command(synthetic,"synthetic")


        
@click.command(help="generate a python configuration for the given table")
@click.argument("table")
@click.argument("version")
def cdm(table,version):
    data_dir = os.path.dirname(carrot.__file__)
    data_dir = f'{data_dir}{os.path.sep}data{os.path.sep}'
    data_dir = f'{data_dir}{os.path.sep}cdm{os.path.sep}BCLINK_EXPORT{os.path.sep}'
    
    #load the details of this cdm objects from the data files taken from OHDSI GitHub
    # - set the table (e.g. person, condition_occurrence,...)  as the index
    #   so that all values associated with the object (name) can be retrieved
    # - then set the field (e.g. person_id, birth_datetime,,) to help with future lookups
    # - just keep information on if the field is required (Yes/No) and what the datatype is (INTEGER,..)
    cdm = pd.read_csv(f'{data_dir}{version}{os.path.sep}export-{table.upper()}.csv',
                      encoding="ISO-8859-1",sep='\t')\
                      .set_index('DESCRIPTION')

    for index,row in cdm.iterrows():
        required = row['REQUIRED'] == "Yes"
        dtype = row['TYPE']
        length = row['LENGTH']
        key = row['KEY']
        
        if not np.isnan(length):
            dtype = f"{dtype}{int(length)}"

        if not np.isnan(key):
            extra = ', pk=True'
        else:
            extra = ''
            
        string = f'self.{index} = DestinationField(dtype="{dtype}", required={required} {extra})'
        print (string)
        
generate.add_command(cdm,"cdm")


@click.command(help="generate a hash token to be used as a salt")
@click.option("length","--length",default=64)
def salt(length):
    salt = secrets.token_hex(length)
    click.echo(salt)

generate.add_command(salt,"salt")



def report_to_xlsx(report,f_out):
    with pd.ExcelWriter(f_out) as writer:  
        for table in report:
            table_name = table['table']
            total = []
            for field in table['fields']:
                field_name = field['field']
                values = field['values']
                data = pd.DataFrame.from_records(values)
                columns = [field_name,'Frequency']
                if data.empty:
                    data = pd.DataFrame(columns=columns)
                else:
                    data.columns=columns
                    total.append(data)
            df = pd.concat(total,axis=1)
            click.echo(df)
            df.to_excel(writer, sheet_name=table_name, index=False)



@click.command(help="generate scan report json from input data")
@click.option("max_distinct_values","--max-distinct-values",
              default=10,
              type=int,
              help='specify the maximum number of distinct values to include in the ScanReport.')
@click.option("min_cell_count","--min-cell-count",
              default=5,
              type=int,
              help='specify the minimum number of occurrences of a cell value before it can appear in the ScanReport.')
@click.option("rows_per_table","--rows-per-table",
              default=100000,
              type=int,
              help='specify the maximum of rows to scan per input data file (table).')
@click.option("randomise","--randomise",
              is_flag=True,
              help='randomise rows')
@click.option("as_type","--as-type","--save-as",
              default=None,
              type=click.Choice(['xlsx','json','latex']),
              help='save the report as a json or xlsx (whiteRabbit style).')
@click.option("f_out","--output-file-name",
              default=None,
              help='specify the output file name (to be used with --save-as)')
@click.option("name","--name",
              required=True,
              help='give a name to the report')
@click.argument("inputs",
                nargs=-1)
def report(inputs,name,max_distinct_values,min_cell_count,rows_per_table,randomise,as_type,f_out):
    skiprows = None
        
    data = []
    for fname in inputs:
        #get the name of the data table
        table_name = os.path.basename(fname)
        #load as a pandas dataframe
        #load it and preserve the original data (i.e. no NaN conversions)
        df = pd.read_csv(fname,
                         dtype=str,
                         keep_default_na=False,
                         nrows=rows_per_table,
                         skiprows=skiprows)
        if randomise:
            df = df.sample(frac=1)

        #get a list of all column (field) names
        column_names = df.columns.tolist()
        fields = []
        #loop over all colimns
        for col in column_names:
            #value count the columns
            series = df[col].value_counts()
            #reduce the size of the value counts depending on specifications of max distinct values 
            if max_distinct_values>0 and len(series)>=max_distinct_values:
                series = series.iloc[:max_distinct_values]

            #if the min cell count is set, remove value counts that are below this threshold
            if not min_cell_count is None:
                series = series[series >= min_cell_count]

            #convert into a frequency instead of value count
            series = (series/len(df)).rename('frequency').round(4) 

            #convert the values to a dictionary 
            frame = series.to_frame()
                        
            values = frame.rename_axis('value').reset_index().to_dict(orient='records')
            #record the value (frequency counts) for this field
            fields.append({'field':col,'values':values})

        meta = {
            'dataset':name,
            'nscanned':len(df),
            'max_distinct_values':max_distinct_values,
            'min_cell_count':min_cell_count
        }
        data.append({'table':table_name,'fields':fields, 'meta':meta})

    if as_type == 'json':
        f_out = f_out if f_out != None else 'ScanReport.json'
        click.echo(json.dumps(data,indent=6))
        with open(f_out, 'w') as f:
            json.dump(data, f,indent=6)
    elif as_type == 'xlsx':
        f_out = f_out if f_out != None else 'ScanReport.xlsx'
        report_to_xlsx(data,f_out)
    elif as_type == 'latex':
        for d in data:
            d = d['fields']
            for d in d:
                df = pd.DataFrame(d['values'])
                if len(df)>0:
                    print (df.to_latex(index=False))
    else:
        click.echo(json.dumps(data,indent=6))
            
generate.add_command(report,"report")


@click.command(help="generate a mapping rules json based on a json scan report")
#@click.option("max_distinct_values","--max-distinct-values",
#              default=10,
#              type=int,
#              help='specify the maximum number of distinct values to include in the ScanReport.')
@click.argument("report")
def mapping_rules(report):

    person_ids = ['ID','PersonID','person_id','Study','Study_ID','CHI','LINKNO']
    date_events = ['date','date_','date_of','time','occurrence','age']
    from difflib import SequenceMatcher
    def get_similar_score(a, b):
        return SequenceMatcher(None, a.lower(), b.lower()).ratio()
    def get_similar_scores(cols,matcher):
        return {b:max([get_similar_score(a,b) for a in matcher]) for b in cols }
    
    domain_lookup = {'Condition':'condition_occurrence','Observation':'observation','Gender':'person'}
    #date_lookup = carrot.cdm.get_cdm_class(destination_
    #print (dir(date_lookup))
    #exit(0)
    def create_hde_from_value(concept,value,table,field,person_id,date_event):
        domain_id = concept['domain_id']
        name = concept['concept_name']
        concept_id = concept['concept_id']
        destination_table = domain_lookup[concept['domain_id']]
        obj = carrot.cdm.get_cdm_class(destination_table)()
        date_events = [k for k,v in obj.get_field_dtypes().items() if v == 'Timestamp']
        date_destination = date_events[0]
        obj = {
            destination_table : {
                name : {
                    "person_id":{
                        "source_table":table,
                        "source_field":person_id,
                    },
                    date_destination:{
                        "source_table":table,
                        "source_field":date_event,
                    },
                    f"{domain_id.lower()}_source_value":{
                        "source_table":table,
                        "source_field":field,
                    },
                    f"{domain_id.lower()}_concept_id":{
                        "source_table":table,
                        "source_field":field,
                        "term_mapping": {
                            value:concept_id
                        }
                    }
                }
            }
        }
        return obj
    
    def find_person_id_and_date_event(fields):
        person_id = [k for k,v in sorted(get_similar_scores(fields,person_ids).items(),key=lambda x: x[1],reverse=True)][0]
        date_event = [k for k,v in sorted(get_similar_scores(fields,date_events).items(),key=lambda x: x[1],reverse=True)][0]
        return person_id,date_event

    def collapse(hdes):
        retval = {}
        for hde in hdes:
            for destination_table,objs in hde.items():
                if destination_table not in retval:
                    retval[destination_table] = {}
                    
                for name,obj in objs.items():
                    if name not in retval[destination_table]:
                         retval[destination_table][name] : {}
                    else:
                        raise Exception(f"{name} already exists")

                    retval[destination_table][name] = obj
        return retval
    
    report = carrot.tools.load_json(report)
    hdes = []
    for table in report:
        fields = [x['field'] for x in table['fields']]
        person_id,date_event = find_person_id_and_date_event(fields)
        for field in table['fields']:
            for value in field['values']:
                concepts = value.get('concepts')
                if not concepts:
                    continue
                for concept in concepts:
                    hde = create_hde_from_value(concept,
                                                value.get('value'),
                                                table['table'],
                                                field['field'],
                                                person_id,
                                                date_event)
                    hdes.append(hde)
    cdm = collapse(hdes)

    name = report[0]['meta']['dataset']
    rules = {
        'metadata':{
            "date_created": str(datetime.datetime.now()),
            "dataset": name
        },
        'cdm':cdm
    }
    click.echo(json.dumps(rules,indent=6))
        

generate.add_command(mapping_rules,"mapping-rules")

@click.command(help="Generate synthetic data from the json format of the scan report")
@click.option("-n","--number-of-events",help="number of rows to generate",required=True,type=int)
@click.option("-o","--output-directory",help="folder to save the synthetic data to",required=True,type=str)
@click.option("--fill-column-with-values",help="select columns to fill values for",multiple=True,type=str)
@click.argument("f_in")
def synthetic_from_json(f_in,number_of_events,output_directory,fill_column_with_values):
    fill_column_with_values = list(fill_column_with_values)
    report = json.load(open(f_in))
    for table in report:
        table_name = table['table']
        fields = table['fields']
        df_synthetic = {}
        for field in fields:
            field_name = field['field']
            values = field['values']
            df = pd.DataFrame.from_records(values)
            if len(df) == 0:
                values = pd.Series([])
            else:
                frequency = df['frequency']
                frequency = number_of_events*frequency / frequency.sum()
                frequency = frequency.astype(int)
                values = df['value'].repeat(frequency).sample(frac=1).reset_index(drop=True)
            values.name = field_name
            
            df_synthetic[field_name] = values
                
        df_synthetic = pd.concat(df_synthetic.values(),axis=1)
        for col_name in fill_column_with_values:
            if col_name in df_synthetic.columns:
                df_synthetic[col_name] = df_synthetic[col_name].reset_index()['index']

        df_synthetic.set_index(df_synthetic.columns[0],inplace=True)

        if not os.path.isdir(output_directory):
            os.makedirs(output_directory)
        fname = f"{output_directory}/{table_name}"

        click.echo(df_synthetic)
        df_synthetic.to_csv(fname)
        click.echo(f"created {fname} with {number_of_events} events")


synthetic.add_command(synthetic_from_json,"json")


            

