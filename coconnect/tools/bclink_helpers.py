import pandas as pd
import io
import time
import json
import os
import coconnect
from coconnect.tools.logger import Logger
from .bash_helpers import BashHelpers


class BCLinkHelpersException(Exception):
    pass

class BCLinkHelpers(BashHelpers):

    def __init__(self,user='bclink',global_ids=None,gui_user='data',database='bclink',dry_run=False,tables=None):
        super().__init__(dry_run=dry_run)
        self.logger = Logger("bclink_helpers")
        self.report = []
        self.user = user
        self.gui_user = gui_user
        self.database = database
        self.table_map = tables
        self.global_ids = global_ids
        
        if self.table_map == None:
            raise BCLinkHelpersException("Table map between the dataset id and the OMOP tables must be defined")

        if self.global_ids == None:
            raise BCLinkHelpersException("A dataset id for the GlobalID mapping must be defined!")

    def create_table(self,table):
        print ("creating table")
        pass

    def check_table_exists(self,table):
        query = f"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = '{table}' )"
               
        cmd=[
            'bc_sqlselect',
            f'--user={self.user}',
            f'--query={query}',
            self.database
        ]
        stdout,_ = self.run_bash_cmd(cmd)
        if stdout == None:
            return True
        return bool(int(stdout.splitlines()[1]))
       
    def get_bclink_table(self,table):
        if table in self.table_map:
            return self.table_map[table]
        elif table == "global_ids":
            return self.global_ids
        
        raise Exception(f"Request look up ofr table {table} which is unknown")


    def drop_duplicates(self,table):
        fields = self.get_fields(table)
        pk = fields[0]
        duplicates = self.get_duplicates(table)
        if len(duplicates) == 0:
            self.logger.info('no duplicates detected')
            return duplicates

        duplicates = ','.join([str(x) for x in duplicates])
        
        cmd=[
            'bc_sqlselect',
            f'--user={self.user}',
            f'--query=DELETE FROM {table} WHERE {pk} IN ({duplicates})',
            self.database
        ]         
        stdout,stderr = self.run_bash_cmd(cmd)
        return duplicates

    def get_duplicates(self,table):
        fields = self.get_fields(table)
        pk = fields[0]
        batch = fields[-1]
        fields = ",".join(fields[1:-1])
        
        cmd=[
            'bc_sqlselect',
            f'--user={self.user}',
            f'--query=SELECT array_agg({pk}) as duplicates FROM {table} GROUP BY {fields} HAVING COUNT(*)>1',
            self.database
        ]
        stdout,stderr = self.run_bash_cmd(cmd)
        if stdout == None:
            return [] 
        duplicates = [
            sorted([int(x) for x in dups[1:-1].split(",")])[1:]
            for dups in stdout.splitlines()[1:]
        ]
        duplicates = sorted(list(set([item for sublist in duplicates for item in sublist])))

        return duplicates

    def get_pk(self,table):
        query = f"SELECT column_name FROM INFORMATION_SCHEMA. COLUMNS WHERE table_name = '{table}' LIMIT 1 "
        cmd = [
            'bc_sqlselect',
            f'--user={self.user}',
            f'--query={query}', 
            self.database
        ]

        stdout,stdin = self.run_bash_cmd(cmd)
        if stdout == None:
            return 'person_id'
        else:
            return stdout.splitlines()[1]
    
    def get_fields(self,table):
        query = f"SELECT * FROM {table} LIMIT 1;"
        cmd = [
            'bc_sqlselect',
            f'--user={self.user}',
            f'--query={query}', 
            self.database
        ]

        stdout,stdin = self.run_bash_cmd(cmd)
        if stdout == None:
            return ['person_id','birth_datetime']
        else:
            return stdout.splitlines()[0].split("\t")
                      
    def get_last_index(self,table):
        pk = self.get_pk(table)
        query=f"SELECT {pk} FROM {table} ORDER BY -{pk} LIMIT 1; "
        cmd = [
            'bc_sqlselect',
            f'--user={self.user}',
            f'--query={query}', 
            self.database
        ]

       
        stdout,stderr = self.run_bash_cmd(cmd)
        if stdout == None:
            return 0
        else:
            last_index = int(stdout.splitlines()[1])
            self.logger.debug(f"Last index for {pk} in table {table} = {last_index}")
            return last_index 
                   
    
    def get_indicies(self):
        reverse = {v:k for k,v in self.table_map.items()}
        retval = {}
        for table in self.table_map.values():
            count=['bc_sqlselect',f'--user={self.user}',f'--query=SELECT count(*) FROM {table}',self.database]
                            
            stdout,stdin = self.run_bash_cmd(count)
            if stdout == None:
                self.get_last_index(table) 
            else:
                counts = int(stdout.splitlines()[1])
                if counts > 0 :
                    retval[reverse[table]] = self.get_last_index(table) + 1

        return retval

    def check_logs(self,job_id,table=None,bclink_table=None):
        cover = f'/data/var/lib/bcos/download/data/job{job_id}/cover.{job_id}'
        if not os.path.exists(cover):
            return False
       
        cmd = f"cat {cover}"
        stdout,stderr = self.run_bash_cmd(cmd)
        if stdout == None:
            return False

        job_id=str(job_id)
        
        report = {
            'job_id':job_id,
            'table':table,
            'bclink_table':bclink_table
        }
             
        
        for msg in stdout.splitlines():
            if 'data row(s) discarded,' in msg:
                self.logger.critical(msg)
                report['dropped_rows'] = msg
            elif 'new row' in msg:
                report['new_rows'] = msg
            elif '>>> From:' in msg:
                report['From'] = msg.split('>>> From:')[1]
            elif '>>> To:' in msg:
                report['To'] = msg.split('>>> To:')[1]
            else:
                self.logger.info_v2(msg)
            
        if report not in self.report:
            self.report.append(report)
        

        return True
        
    def clean_table(self,table):
        clean = f'datasettool2 delete-all-rows {table} --database={self.database}'
        stdout,stderr = self.run_bash_cmd(clean)
        
        if stdout == None and stderr==None:
            return

        for msg in stderr.splitlines():
            self.logger.warning(msg)
                   
    def clean_tables(self,tables=None):
        for table in self.table_map.values():
            if tables is not None:
                if not table in tables:
                    continue

            self.logger.info(f"Cleaning table {table}")
            self.clean_table(table)
       
        if tables is not None:
            if not self.global_ids in tables:
                return

        self.logger.info(f"Cleaning existing person ids in {self.global_ids}")
        self.clean_table(self.global_ids)
            
    def get_table_jobs(self,table,head=1):
        cmd = f'datasettool2 list-updates --dataset={table} --user={self.gui_user} --database={self.database}'
        status,_ = self.run_bash_cmd(cmd)
        if status == None:
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
   
    def get_global_ids(self,f_out):
        # todo: chunking needs to be developed here!
        _dir = os.path.dirname(f_out)
        if not os.path.exists(_dir):
            self.logger.info(f'making output folder {_dir} to insert existing masked ids')
            os.makedirs(_dir)
   
        query=f"SELECT * FROM {self.global_ids} "
        cmd=['bc_sqlselect',f'--user={self.user}',f'--query={query}',self.database]
        
        stdout,stderr = self.run_bash_cmd(cmd)
        if stdout == None:
            return None
        if len(stdout.splitlines()) == 0:
            return None
            
        df_ids = pd.read_csv(io.StringIO(stdout),
                             sep='\t').set_index("SOURCE_SUBJECT")
        
        df_ids.to_csv(f_out,sep='\t')
        return f_out
                       
    
    def check_global_ids(self,output_directory,chunksize=10):
        data_file = f'{output_directory}/person.tsv'
        if not os.path.exists(data_file):
            self.logger.warning(f"{output_directory}/person.tsv file does not exist")
            self.logger.warning("skipping global id check")
            return True

        data = coconnect.tools.load_csv({"ids":f"{output_directory}/global_ids.tsv"},
                                        sep='\t',
                                        chunksize=100)
        
        while True:
            query=f"select exists(select 1 from {self.global_ids} where TARGET_SUBJECT in ({_list}) )"
            cmd=['bc_sqlselect',f'--user={self.user}',f'--query={query}',self.database]
            stdout,stderr = self.run_bash_cmd(cmd)
            if stdout == None:
                exists = False   
            else:    
                exists = bool(int(stdout.splitlines()[1]))
            
            if exists:
                #if any target IDs (hashed!) already exist in the table... 
                #check if the pairing is different
                _list = ','.join([f"('{s}','{t}')" for s,t in data["ids"].values])
                self.logger.debug("getting IDs that overlap")
                query=f"select SOURCE_SUBJECT,SOURCE_SUBJECT from {self.global_ids} where (SOURCE_SUBJECT,TARGET_SUBJECT) in ({_list}) "
                cmd=['bc_sqlselect',f'--user={self.user}',f'--query={query}',self.database]
                stdout,stderr = self.run_bash_cmd(cmd)
                info = pd.read_csv(io.StringIO(stdout),
                                   sep='\t').set_index("SOURCE_SUBJECT")
                self.logger.error(info)
                return False

            try:
                data.next()
            except StopIteration:
                break
   
        return True
        
   
    def load_global_ids(self,output_directory):
        data_file = f'{output_directory}/global_ids.tsv'
        if not os.path.exists(data_file):
            #raise FileExistsError(
            self.logger.error(f"Cannot find global_ids.tsv in output directory: {output_directory}")
            return 
           
        cmd = ['dataset_tool', '--load',f'--table={self.global_ids}',f'--user={self.gui_user}',
               f'--data_file={data_file}','--support','--bcqueue',self.database]
        
        stdout,stderr = self.run_bash_cmd(cmd)
        if not stdout == None:
            for msg in stdout.splitlines():
                self.logger.info(f"submitted job to bclink queue: {msg}")

        table_name = self.global_ids
        stats = self.get_table_jobs(table_name)
        if stats is None:
            #is a dry run, just test this
            self.check_logs(0)
        else:
            job_id = stats.iloc[0]['JOB']
            while True:
                stats = self.get_table_jobs(table_name)
                self.logger.info(stats)
                self.logger.info(f"Getting log for {table_name} id={job_id}")
                success = self.check_logs(job_id,'global_ids',table_name)
                if success:
                    break
                else:
                    self.logger.debug(f"Didn't find the log for {table_name} id={job_id} yet, job still running. Trying again in 5 seconds..")
                    time.sleep(5)
        

    def load_tables(self,output_directory,tables_to_process=None):
        for table,tablename in self.table_map.items():
            if tables_to_process is not None:
                if table not in tables_to_process:
                    continue

            data_file = f'{output_directory}/{table}.tsv'
            if not os.path.exists(data_file):
                #raise FileExistsError(
                self.logger.error(f"Cannot find {table}.tsv in output directory: {output_directory}")
                continue

            cmd = ['dataset_tool', '--load',f'--table={tablename}',f'--user={self.gui_user}',
                   f'--data_file={data_file}','--support','--bcqueue',self.database]
            
            stdout,stderr = self.run_bash_cmd(cmd)
            if not stdout == None:
                for msg in stdout.splitlines():
                    self.logger.info(f"submitted job to bclink queue: {msg}")


        for table,table_name in self.table_map.items():
            if tables_to_process is not None:
                if table not in tables_to_process:
                    continue
            self.logger.debug(f"Checking jobs submitted for {table_name}")
            stats = self.get_table_jobs(table_name)
            if stats is None:
                #is a dry run, just test this
                self.check_logs(0)
            else:
                job_id = stats.iloc[0]['JOB']
                while True:
                    stats = self.get_table_jobs(table_name)
                    self.logger.info(stats)
                    self.logger.info(f"Getting log for {table_name} id={job_id}")
                    success = self.check_logs(job_id,table,table_name)
                    if success:
                        break
                    else:
                        self.logger.warning(f"Didn't find the log for {table_name} id={job_id} yet, job still running.")
                        time.sleep(1)

        self.print_summary()

    def print_report(self):
        if self.report:
            self.logger.info(json.dumps(self.report,indent=6))

    def print_summary(self):
        info = {}
        for table,table_name in self.table_map.items():
            if table_name == None:
                continue
            cmd=['bc_sqlselect',f'--user={self.user}',f'--query=SELECT count(*) FROM {table_name}',self.database]
            
            stdout,stderr = self.run_bash_cmd(cmd)
            if not stdout == None:
                
                count = stdout.splitlines()[1]
                info[table] = {'bclink_table':table_name,
                               'nrows':count}
                if table == 'person' and self.global_ids:
                    cmd=['bc_sqlselect',
                         f'--user={self.user}',
                         f'--query=SELECT count(*) FROM {self.global_ids}',
                         self.database]
                    stdout,stderr = self.run_bash_cmd(cmd)
                    count = stdout.splitlines()[1]
                    info[table]['global_ids'] = {
                        'bclink_table':self.global_ids,
                        'nrows':count
                    }
      
                    #to-do warn/error if counts differ betwene person and global ids table
        
        if info:
            self.logger.info("======== BCLINK SUMMARY ========")
            self.logger.info(json.dumps(info,indent=6))
              

    def remove_table(self,fname):
        self.logger.info(f"Called remove_table on {fname}")
        table = os.path.splitext(os.path.basename(fname))[0]
        data = coconnect.tools.load_csv({table:{'fields':[0],'file':fname}},
                                        sep='\t',
                                        chunksize=1000)

        if table in self.table_map:
            bc_table = self.table_map[table]
        else:#if table == 'global_ids':
            return
                    
        pk = self.get_pk(bc_table)
        self.logger.debug(f"will remove {bc_table} using primary-key={pk}")
            
        while True:
            indices_to_delete = ','.join(data[table].iloc[:,0].values)
            self.logger.debug(f"removing {len(indices_to_delete)} indices from {bc_table}")
            query=f"DELETE FROM {bc_table} WHERE {pk} IN ({indices_to_delete}) "
            cmd=['bc_sqlselect',f'--user={self.user}',f'--query={query}',self.database]
            
            stdout,stderr = self.run_bash_cmd(cmd)
            
            try:
                data.next()
            except StopIteration:
                break
