from .bash_helpers import run_bash_cmd
import pandas as pd
import io
import os
from coconnect.tools.logger import Logger

class BCLinkHelpers:

    def __init__(self,user='bclink',gui_user='data',database='bclink',table_map=None):
        self.logger = Logger("bclink_helpers")
        self.user = user
        self.gui_user = gui_user
        self.database = database
        self.table_map = table_map
        if self.table_map == None:
            raise Exception("Table Map must be defined")
    
    def get_duplicates(self,table,fields,dry_run=False):
        pk = fields[0]
        fields = ",".join(fields[1:])
        cmd=[
            'bc_sqlselect',
            f'--user={self.user}',
            f'--query=SELECT array_agg({pk}) as duplicates FROM {table} GROUP BY {fields} HAVING COUNT(*)>1',
            self.database
        ]
        return run_bash_cmd(cmd)
       

    def get_indicies(self,dry_run=False):
        reverse = {v:k for k,v in self.table_map.items()}
        retval = {}
        for table in self.table_map.values():
            count=['bc_sqlselect',f'--user={self.user}',f'--query=SELECT count(*) FROM {table}',self.database]
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

    def check_logs(self,job_id,dry_run=False):
        cover = f'/data/var/lib/bcos/download/data/job{job_id}/cover.{job_id}'
        if not os.path.exists(cover):
            return None
        cmd = f"cat {cover}"
        if dry_run:
            cmd = 'echo '+cmd
        stdout,stderr = run_bash_cmd(cmd)
        return stdout.splitlines()
        
    def clean_table(self,table,dry_run=False):
        clean = f'datasettool2 delete-all-rows {table} --database={self.database}'
        if dry_run:
            clean = 'echo '+clean
        stdout,stderror = run_bash_cmd(clean)
        for msg in stdout.splitlines():
            self.logger.info(msg)
        for msg in stderror.splitlines():
            self.logger.warning(msg)
           
    def clean_tables(self,dry_run=False):
        for table in self.table_map.values():
            self.clean_table(table,dry_run=dry_run)
            
    def get_table_jobs(self,table,head=5,dry_run=False):
        cmd = f'datasettool2 list-updates --dataset={table} --user={self.gui_user} --database={self.database}'
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
    
    def load_tables(self,output_directory,dry_run=False):
        msgs=[]
        for table,tablename in self.table_map.items():
            data_file = f'{output_directory}/{table}.tsv'
            if not os.path.exists(data_file):
                raise FileExistsError(f"Cannot find {table}.tsv in output directory: {output_directory}")

            cmd = ['dataset_tool', '--load',f'--table={tablename}',f'--user={self.gui_user}',
                   f'--data_file={data_file}','--support','--bcqueue',self.database]
            if dry_run:
                cmd.insert(0,'echo')
            stdout,stderr = run_bash_cmd(cmd)
            msgs = msgs + stdout.splitlines()
        return msgs
