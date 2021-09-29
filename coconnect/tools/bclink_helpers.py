from .bash_helpers import run_bash_cmd
import pandas as pd
import io
import os

def get_indicies(tables):
    reverse = {v:k for k,v in tables.items()}
    retval = {}
    for table in tables.values():
        count=['bc_sqlselect','--user=bclink',f'--query=SELECT count(*) FROM {table}','bclink']
        stdout,stdin = run_bash_cmd(count)
        last_index = int(stdout.splitlines()[1])
        if last_index > 0 :
            retval[reverse[table]] = last_index
    return retval
        
def clean_table(table):
    clean = f'datasettool2 delete-all-rows {table} --database=bclink'
    return run_bash_cmd(clean)
    
def clean_tables(tables):
    for table in tables.values():
        clean_table(table)

def get_table_jobs(table,head=5):
    cmd = f'datasettool2 list-updates --dataset={table} --user=data --database=bclink'
    status,_ = run_bash_cmd(cmd)
    info = pd.read_csv(io.StringIO(status),
                       sep='\t',
                       usecols=['BATCH',
                                'UPDDATE',
                                'UPD_COMPLETION_DATE',
                                'JOB',
                                'STATUS',
                                'ACTION'])
    if head is not None:
        info = info.head(head)
    return info
    
def load_tables(table_map,output_directory):
    msgs=[]
    for table,tablename in table_map.items():
        data_file = f'{output_directory}/{table}.tsv'
        if not os.path.exists(data_file):
            raise FileExistsError(f"Cannot find {table}.tsv in output directory: {output_directory}")

        cmd = ['dataset_tool', '--load',f'--table={tablename}','--user=data',
               f'--data_file={data_file}','--support','--bcqueue','bclink']

        stdout,stderr = run_bash_cmd(cmd)
        msgs = msgs + stdout.splitlines()
    return msgs
