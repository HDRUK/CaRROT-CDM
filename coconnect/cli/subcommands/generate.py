import os
import click
import json
import coconnect
import pandas as pd
import numpy as np
import requests

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
@click.option("-t","--token",help="specify the coconnect_token for accessing the CCOM website",type=str,default=None)
@click.option("-u","--url",help="url endpoint for the CCOM website to ping",
              type=str,
              default="https://ccom.azurewebsites.net")
def ccom(report_id,number_of_events,output_directory,
         fill_column_with_values,token,
         url):
    token = os.environ.get("COCONNECT_TOKEN") or token
    if token == None:
        raise MissingToken("you must use the option --token or set the environment variable COCONNECT_TOKEN to be able to use this functionality. I.e  export COCONNECT_TOKEN=12345678 ")

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.119 Safari/537.36",
        "Content-type": "application/json",
        "charset": "utf-8",
        "Authorization": f"Token {token}"
    }

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
        
    tables = {
        table['name']:table['id']
        for table in response.json()
        }

    for name,_id in tables.items():
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

        df_synthetic.set_index(df_synthetic.columns[0],inplace=True)
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


synthetic.add_command(xlsx,"xlsx")
synthetic.add_command(ccom,"ccom")
generate.add_command(synthetic,"synthetic")


        
@click.command(help="generate a python configuration for the given table")
@click.argument("table")
@click.argument("version")
def cdm(table,version):
    data_dir = os.path.dirname(coconnect.__file__)
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
