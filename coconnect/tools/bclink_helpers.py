from .bash_helpers import run_bash_cmd
import pandas as pd
import io
import time
import json
import os
from coconnect.tools.logger import Logger

class BCLinkHelpers:

    def __init__(self,user='bclink',gui_user='data',database='bclink',dry_run=False,tables=None):
        self.logger = Logger("bclink_helpers")
        self.user = user
        self.gui_user = gui_user
        self.database = database
        self.dry_run = dry_run
        self.table_map = tables
        if self.table_map == None:
            raise Exception("Table Map must be defined")
    
    def get_duplicates(self,table,fields):
        pk = fields[0]
        fields = ",".join(fields[1:])
        cmd=[
            'bc_sqlselect',
            f'--user={self.user}',
            f'--query=SELECT array_agg({pk}) as duplicates FROM {table} GROUP BY {fields} HAVING COUNT(*)>1',
            self.database
        ]
        return run_bash_cmd(cmd)
       

    def get_pk(self,table):
        query = f"SELECT column_name FROM INFORMATION_SCHEMA. COLUMNS WHERE table_name = '{table}' LIMIT 1 "
        cmd = [
            'bc_sqlselect',
            f'--user={self.user}',
            f'--query={query}', 
            self.database
        ]

        if self.dry_run:
            cmd.insert(0,'echo')

        stdout,stdin = run_bash_cmd(cmd)
        if self.dry_run:
            for msg in stdout.splitlines():
                self.logger.critical(msg)
            return 'person_id'
        else:
            return stdout.splitlines()[1]
                      
    def get_last_index(self,table):
        pk = self.get_pk(table)
        query=f"SELECT {pk} FROM {table} ORDER BY -{pk} LIMIT 1; "
        cmd = [
            'bc_sqlselect',
            f'--user={self.user}',
            f'--query={query}', 
            self.database
        ]

        if self.dry_run:
            cmd.insert(0,'echo')

        stdout,stderr = run_bash_cmd(cmd)
        if self.dry_run:
            for msg in stdout.splitlines():
                self.logger.critical(msg)
            return 0
        else:
            last_index = int(stdout.splitlines()[1])
            self.logger.info(f"Last index for {pk} in table {table} = {last_index}")
            return last_index 
                   
    
    def get_indicies(self):
        reverse = {v:k for k,v in self.table_map.items()}
        retval = {}
        for table in self.table_map.values():
            count=['bc_sqlselect',f'--user={self.user}',f'--query=SELECT count(*) FROM {table}',self.database]
            if self.dry_run:
                count.insert(0,'echo')
                
            stdout,stdin = run_bash_cmd(count)
            if self.dry_run:
                for msg in stdout.splitlines():
                    self.logger.critical(msg)
                self.get_last_index(table) 
            else:
                counts = int(stdout.splitlines()[1])
                if counts > 0 :
                    retval[reverse[table]] = self.get_last_index(table) + 1

        return retval

    def check_logs(self,job_id):
        cover = f'/data/var/lib/bcos/download/data/job{job_id}/cover.{job_id}'
        if not self.dry_run and not os.path.exists(cover):
            return False
        cmd = f"cat {cover}"
        if self.dry_run:
            cmd = 'echo '+cmd
        stdout,stderr = run_bash_cmd(cmd)
        for msg in stdout.splitlines():
            if self.dry_run:
                self.logger.critical(msg)
            else:
                self.logger.info(msg)
        return True
        
    def clean_table(self,table):
        clean = f'datasettool2 delete-all-rows {table} --database={self.database}'
        if self.dry_run:
            clean = 'echo '+clean
        stdout,stderr = run_bash_cmd(clean)
       
        if self.dry_run:
            for msg in stdout.splitlines():
                self.logger.critical(msg)
        else:
            for msg in stderr.splitlines():
                self.logger.warning(msg)
                   
    def clean_tables(self):
        for table in self.table_map.values():
            self.logger.info(f"Cleaning table {table}")
            self.clean_table(table)
       
            
    def get_table_jobs(self,table,head=5):
        cmd = f'datasettool2 list-updates --dataset={table} --user={self.gui_user} --database={self.database}'
        if self.dry_run:
            cmd = 'echo '+cmd
        status,_ = run_bash_cmd(cmd)
        if self.dry_run:
            for msg in status.splitlines():
                self.logger.critical(msg)
            return
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
    
    def load_tables(self,output_directory):
        for table,tablename in self.table_map.items():
            data_file = f'{output_directory}/{table}.tsv'
            if not os.path.exists(data_file):
                raise FileExistsError(f"Cannot find {table}.tsv in output directory: {output_directory}")

            cmd = ['dataset_tool', '--load',f'--table={tablename}',f'--user={self.gui_user}',
                   f'--data_file={data_file}','--support','--bcqueue',self.database]
            if self.dry_run:
                cmd.insert(0,'echo')
            stdout,stderr = run_bash_cmd(cmd)
            for msg in stdout.splitlines():
                if self.dry_run:
                    self.logger.critical(msg)
                else:
                    self.logger.info(f"submitted job to bclink queue: {msg}")


        for table_name in self.table_map.values():
            self.logger.info(f"Checking jobs submitted for {table_name}")
            stats = self.get_table_jobs(table_name)
            if stats is None:
                #is a dry run, just test this
                self.check_logs(0)
            else:
                self.logger.info(stats)
                job_id = stats.iloc[0]['JOB']
                while True:
                    self.logger.info(f"Getting log for {table_name} id={job_id}")
                    success = self.check_logs(job_id)
                    if success:
                        break
                    else:
                        self.logger.warning(f"Didn't find the log for {table_name} id={job_id} yet, job still running.")
                        time.sleep(1)
    
        info = {}
        for table,table_name in self.table_map.items():
            cmd=['bc_sqlselect',f'--user={self.user}',f'--query=SELECT count(*) FROM {table_name}',self.database]
            if self.dry_run:
                cmd.insert(0,'echo')

            stdout,stderr = run_bash_cmd(cmd)
            if self.dry_run:
                for msg in stdout.splitlines():
                    self.logger.critical(msg)
            else:
                count = stdout.splitlines()[1]
                info[table] = {'bclink_table':table_name,
                               'nrows':count}
        if not self.dry_run:
            self.logger.info("======== SUMMARY ========")
            self.logger.info(json.dumps(info,indent=6))
                    
