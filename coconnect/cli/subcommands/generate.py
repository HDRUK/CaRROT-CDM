import os
import click
import coconnect
import pandas as pd
    
@click.group(help='Commands to generate helpful files.')
def generate():
    pass

@click.command(help="generate synthetic data from a ScanReport")
@click.argument("report")
@click.option("-n","--number-of-events",help="number of rows to generate",required=True,type=int)
@click.option("-o","--output-directory",help="folder to save the synthetic data to",required=True,type=str)
@click.option("--fill-column-with-values",help="select columns to fill values for",multiple=True,type=str)
def synthetic(report,number_of_events,output_directory,fill_column_with_values):
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


@click.command(help="generate a python configuration for the given table")
@click.argument("table")
@click.argument("version")
def cdm(table,version):
    data_dir = os.path.dirname(coconnect.__file__)
    data_dir = f'{data_dir}/data/'

    version = 'v'+version.replace(".","_")
    
    #load the details of this cdm objects from the data files taken from OHDSI GitHub
    # - set the table (e.g. person, condition_occurrence,...)  as the index
    #   so that all values associated with the object (name) can be retrieved
    # - then set the field (e.g. person_id, birth_datetime,,) to help with future lookups
    # - just keep information on if the field is required (Yes/No) and what the datatype is (INTEGER,..)
    cdm = pd.read_csv(f'{data_dir}/cdm/OMOP_CDM_{version}.csv',encoding="ISO-8859-1")\
                 .set_index('table')\
                 .loc[table].set_index('field')[['required', 'type']]

    for index,row in cdm.iterrows():
        required = row['required'] == "Yes"
        dtype = row['type']
        string = f'self.{index} = DestinationField(dtype="{dtype}", required={required})'
        print (string)
    
generate.add_command(cdm,"cdm")
generate.add_command(synthetic,"synthetic")
