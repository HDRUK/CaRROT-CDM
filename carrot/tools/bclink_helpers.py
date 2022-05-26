import pandas as pd
import io
import time
import json
import os
import carrot
from carrot.tools.logger import Logger
from .bash_helpers import BashHelpers
from carrot.cdm.objects import get_cdm_tables


def get_default_global_id_name():
    return 'person_ids'

def get_default_tables():
    _dict = {k:k for k in get_cdm_tables().keys()}
    _dict[get_default_global_id_name()] = get_default_global_id_name()
    return _dict


class BCLinkHelpersException(Exception):
    pass

class BCLinkHelpers(BashHelpers,Logger):

    def __init__(self,user='bclink',clean=False,check=True,gui_user='data',database='bclink',dry_run=False,tables=get_default_tables()):
        super().__init__(dry_run=dry_run)

        self.report = []
        self.user = user
        self.gui_user = gui_user
        self.database = database
        self.table_map = tables
        if self.table_map == None:
            raise BCLinkHelpersException("Table map between the dataset id and the OMOP tables must be defined")

        if check:
            self.check_tables()
        if clean:
            self.clean_tables()
        if check:
            self.print_summary()
        
    def get_table_map(self):
        return self.table_map

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
            return  True
        return bool(int(stdout.splitlines()[1]))

    def check_tables(self):
        for table,table_name in self.table_map.items():
            exists = self.check_table_exists(table_name)
            if exists:
                self.logger.info(f"{table_name} ({table}) already exists --> all good")
            else:
                self.logger.error(f"{table_name} which is to be used for {table} does not exist!")
                #self.create_table(table_name,table)
                raise BCLinkHelpersException(f"{table_name} does not exist")

    def create_table(self,table_name,table):
        table_upper = table.upper()
        table_name_upper = table_name.upper()
        cmd = [
            'dataset_tool', 
            '--create',
            f'--table={table_name}',
            f'--setname={table_name_upper}',
            f'--user={self.gui_user}',
            f'--form={table_upper}',
            self.database
        ]
        stdout,stderr = self.run_bash_cmd(cmd)
        if stdout:
            self.logger.info(stdout)

    def create_tables(self):
        for table,table_name in self.table_map.items():
            exists = self.check_table_exists(table_name)
            if exists:
                self.logger.info(f"{table_name} ({table}) already exists, not creating")
                continue
            self.create_table(table_name,table)
          
    def get_table(self,table):
        query = f"SELECT * FROM {table}"
               
        cmd=[
            'bc_sqlselect',
            f'--user={self.user}',
            f'--query={query}',
            self.database
        ]
        stdout,_ = self.run_bash_cmd(cmd)
        if stdout == None:
            return None

        df = pd.read_csv(io.StringIO(stdout),sep='\t')
        df.columns = [x.lower() for x in df.columns]
        #df = df.drop('batch',axis=1)
        return df
       
  
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
            pk = stdout.splitlines()[1]
            self.logger.info(f"got pk {pk}")
            return pk
            
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
        the_dict = {k:v for k,v in self.table_map.items() if not k == get_default_global_id_name()}
        reverse = {v:k for k,v in the_dict.items() }
        retval = {}
        for table in the_dict.values():
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
   
    def get_global_ids(self):
        name = get_default_global_id_name()
        if name not in self.table_map.keys():
            self.logger.warning(f"No table for getting existing person ids ({name}) has been defined")
            return None
        global_ids = self.table_map[name]
   
        query=f"SELECT * FROM {global_ids} "
        cmd=['bc_sqlselect',f'--user={self.user}',f'--query={query}',self.database]
        
        stdout,stderr = self.run_bash_cmd(cmd)
        #if stdout == None:
        #    return None
        #if len(stdout.splitlines()) == 0:
        #    return None

        return stdout#io.StringIO(stdout)
        
        #df_ids = pd.read_csv(io.StringIO(stdout),
        #                     sep='\t').set_index("SOURCE_SUBJECT")
       # 
       # df_ids.to_csv(f_out,sep='\t')
       # return f_out

    def check_global_ids(self,output_directory,chunksize=10):

        if not self.global_ids:
            return True

        data_file = f'{output_directory}/person.tsv'
        if not os.path.exists(data_file):
            self.logger.warning(f"{output_directory}/person.tsv file does not exist")
            self.logger.warning("skipping global id check")
            return True

        data = carrot.tools.load_csv({"ids":f"{output_directory}/global_ids.tsv"},
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

        if self.global_ids == None:
            return

        data_file = f'{output_directory}{os.path.sep}global_ids.tsv'
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

    def load_table(self,f_out,destination_table):

        try:
            tablename = self.table_map[destination_table]
        except KeyError:
            self.logger.error(f"table {destination_table} unknown in {self.table_map.keys()}")
            return
        
        if not os.path.exists(f_out):
            self.logger.error(f"Cannot find {f_out} to load to bclink.")
            return
                
        cmd = ['dataset_tool', '--load',f'--table={tablename}',f'--user={self.gui_user}',
               f'--data_file={f_out}','--support','--bcqueue',self.database]
        
        stdout,stderr = self.run_bash_cmd(cmd)
        if not stdout == None:
            for msg in stdout.splitlines():
                self.logger.info(f"submitted job to bclink queue: {msg}")
            
        stats = self.get_table_jobs(tablename)
        if stats is None:
            #is a dry run, just test this
            #self.check_logs(0)
            pass
        else:
            job_id = stats.iloc[0]['JOB']
            self.logger.info(f"running job {job_id}")
            #self.check_logs(job_id)
            return job_id

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
            if stdout == None:
                continue
                
            count = stdout.splitlines()[1]
            info[table] = {'bclink_table':table_name,
                           'nrows':count}            
        
        if info:
            self.logger.info("======== BCLINK SUMMARY ========")
            self.logger.info(json.dumps(info,indent=6))
              

    def remove_table(self,fname):
        self.logger.info(f"Called remove_table on {fname}")
        table = os.path.splitext(os.path.basename(fname))[0].split('.')[0]
        data = carrot.tools.load_csv({table:{'fields':[0],'file':fname}},
                                        sep='\t',
                                        chunksize=1000)


        if not table in self.table_map:
            raise KeyError(f"{table} doesnt exist in bclink mapping")

        bc_table = self.table_map[table]

                    
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
