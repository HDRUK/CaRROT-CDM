from .bash_helpers import run_bash_cmd
import pandas as pd
import io
import os

def get_indicies(tables,dry_run=False):
    reverse = {v:k for k,v in tables.items()}
    retval = {}
    for table in tables.values():
        count=['bc_sqlselect','--user=bclink',f'--query=SELECT count(*) FROM {table}','bclink']
        if dry_run:
            count.insert(0,'echo')

        stdout,stdin = run_bash_cmd(count)

        if dry_run:
            retval[table] = stdout.splitlines()[0]
        else:
            last_index = int(stdout.splitlines()[1])
            if last_index > 0 :
                retval[reverse[table]] = last_index

    return retval

def check_logs(job_id,dry_run=False):
    cover = f'/data/var/lib/bcos/download/data/job{job_id}/cover.{job_id}'
    if not os.path.exists(cover):
        return None
    cmd = f"cat {cover}"

    if dry_run:
        cmd = 'echo '+cmd

    stdout,stderr = run_bash_cmd(cmd)
    return stdout.splitlines()
        
def clean_table(table,dry_run=False):
    clean = f'datasettool2 delete-all-rows {table} --database=bclink'
    if dry_run:
        clean = 'echo '+clean
    return run_bash_cmd(clean)
    
def clean_tables(tables,dry_run=False):
    for table in tables.values():
        clean_table(table,dry_run=dry_run)

def get_table_jobs(table,head=5,dry_run=False):
    cmd = f'datasettool2 list-updates --dataset={table} --user=data --database=bclink'
    if dry_run:
        cmd = 'echo '+cmd
    status,_ = run_bash_cmd(cmd)
    if dry_run:
        return status
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
    
def load_tables(table_map,output_directory,dry_run=False):
    msgs=[]
    for table,tablename in table_map.items():
        data_file = f'{output_directory}/{table}.tsv'
        if not os.path.exists(data_file):
            raise FileExistsError(f"Cannot find {table}.tsv in output directory: {output_directory}")

        cmd = ['dataset_tool', '--load',f'--table={tablename}','--user=data',
               f'--data_file={data_file}','--support','--bcqueue','bclink']
        if dry_run:
            cmd.insert(0,'echo')

        stdout,stderr = run_bash_cmd(cmd)
        msgs = msgs + stdout.splitlines()
    return msgs
