from datetime import timedelta
from textwrap import dedent
from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import  DummyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.task_group import TaskGroup
        

from airflow.utils.dates import days_ago
from airflow.models import Variable

import configparser

import re
import requests
import glob
import os
import json
import coconnect 

default_dir = os.path.dirname(coconnect.__file__)+"/data/test/"


f_debug_level = 3
coconnect.params['debug_level'] = f_debug_level

f_schedule = {'days': 5}

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}


def get_headers(token):
    return {
        "User-Agent": "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.119 Safari/537.36",
        "Content-type": "application/json",
        "charset": "utf-8",
        "Authorization": f"Token {token}"
    }

def create_template(f_name,f_inputs,f_output_folder,f_rules,f_schedule={'weeks':4}):

    f_name = f_name.replace(" ","_")
    
    template = f'''
from coconnect.workflows.airflow import create_dag
import json
import coconnect

dag = create_dag("{f_name}",
                f_inputs="{f_inputs}",
                f_output_folder="{f_output_folder}",
                f_rules="{f_rules}")

'''

    _dir = os.environ.get('AIRFLOW_HOME')

    config = configparser.RawConfigParser()
    config.read(f'{_dir}/airflow.cfg')
    dags_folder = dict(config.items('core'))['dags_folder']
    
    _dir = f"{dags_folder}/etl_dags"
    os.makedirs(_dir,exist_ok=True)

    f_name = f"{_dir}/{f_name}.py"
    with open(f_name,"w") as f:
        f.write(template)

    return f_name

def get_dir():
    _dir = os.environ.get('AIRFLOW_HOME')
    return f'{_dir}/data'

def get_json(_id,url,token):

    _url = f'{url}/api/json/?id={_id}'
    headers = get_headers(token)
    response = requests.get(_url,headers=headers).json()[0]
    print (json.dumps(response,indent=6))

    dataset = response['metadata']['dataset']
    created = response['metadata']['date_created']

    _dir = get_dir()
    _dir = f'{_dir}/{dataset}/{created}'
    os.makedirs(_dir,exist_ok=True)

    f_name = f"{_dir}/rules.json"
    with open(f_name,'w') as outfile:
        json.dump(response, outfile, indent=6)
    
    return {'name':dataset,'rules':f_name,'dir':_dir}



def make_dag(**kwargs):
    ti = kwargs['ti']
    f_rules = ti.xcom_pull(task_ids='get_json')['rules']
    f_outputs = ti.xcom_pull(task_ids='get_json')['dir']+"/output_data/"
    f_inputs = ti.xcom_pull(task_ids='get_json')['dir']+"/input_data/"
    f_name = ti.xcom_pull(task_ids='get_json')['name']
    
    return create_template(f_name,f_inputs,f_outputs,f_rules)


def decide_trigger(**kwargs):
    return 'finish'

@dag(default_args=default_args,
     schedule_interval=None,
     start_date=days_ago(2),
     tags=['manager'])
def coconnect_report_manager():
    start = BashOperator(
        task_id=f"start",
        bash_command='echo Starting workflow for report_id = {{ dag_run.conf["report_id"] }} '
    )
    
    download_json = PythonOperator(
        task_id=f"get_json",
        python_callable=get_json,
        op_kwargs={
            '_id':'{{ dag_run.conf["report_id"] }}',
            'url':' {{ dag_run.conf["url"] }}',
            'token': '{{ dag_run.conf["token"] }}'
        }
    )
    
    run_synthetic = BashOperator(
        task_id=f"generate_synthetic",
        bash_command='coconnect generate synthetic ccom --token {{ dag_run.conf["token"] }} \
        --url {{ dag_run.conf["url"] }} --number-of-events 100 --report-id ''{{ dag_run.conf["report_id"] }}\
        --output-directory \"{{ ti.xcom_pull("get_json")["dir"] }}/input_data/\" '
    )
    
    create_dag = PythonOperator(
        task_id=f"make_dag",
        python_callable=make_dag
    )

    decide = BranchPythonOperator(
        task_id=f"decision_trigger",
        python_callable=decide_trigger
    )
    
    trigger_dag = TriggerDagRunOperator(
        task_id=f"trigger_dag",
        trigger_dag_id='{{ ti.xcom_pull("make_dag") }}',
        wait_for_completion=True,
        retries=0
    )

    finish = BashOperator(
        task_id=f"finish",
        bash_command='echo done!'
    )

    
    start >> download_json >> run_synthetic >> create_dag >> decide
    trigger_dag >> finish
    decide >> [ trigger_dag, finish] 
    
    

def get_reports(url,token):

    headers = get_headers(token)
    
    response = requests.get(url,headers=headers).json()
    reports = {}
    for report in response:
        if report['hidden'] == False:
            continue
        reports[str(report['id'])] = report['dataset']

    return reports

@dag(default_args=default_args,
     schedule_interval=None,
     start_date=days_ago(2),
     tags=['manager'])
def coconnect_report_getters():
    op = PythonOperator(task_id=f"trigger_reports",
                        python_callable=get_reports,
                        op_kwargs={'url':'{{ dag_run.conf["url"] }} ','token':'{{ dag_run.conf["token"] }}'})


def create_simple_dag(dag_name,f_inputs,f_output_folder,f_rules,f_schedule={'weeks':4}):
    
    with DAG(
            dag_name,
            default_args=default_args,
            description='A simple tutorial DAG',
            schedule_interval=timedelta(days=1),
            start_date=days_ago(2),
            tags=['example'],
    ) as dag:

        salt = BashOperator(
            task_id='run_pseudonymisation',
            bash_command=f'echo "salting dataset"'
        )

        run = BashOperator(
            task_id='run_python_tool',
            bash_command=f'coconnect map run --rules {f_rules} --output-folder {f_output_folder} {f_inputs} ',
        )

        config = json.load(open(f_rules))
        tables = list(config['cdm'].keys())

        templated_command = dedent(
            """
        {% for table in params.tables %}
            echo {{ table }} 
            echo datasettool2 delete-all-rows {{ table }}  --database=bclink
            echo dataset_tool --load --table={{ table }} --user=data --data_file="{{ params.output }}{{ table }}.tsv" --support  --bcqueue --bcqueue-res-path=./logs/{{ table }}  bclink
        {% endfor %}
           """
        )

        upload = BashOperator(
            task_id=f'insert_into_bclink',
            bash_command=templated_command,
            params={'tables': tables,"output":f_output_folder},
        )
        salt >> run >> upload
        
        return dag
    

def create_dag(dag_name,f_inputs,f_output_folder,f_rules,f_schedule={'weeks':4}):
    
    f_input_map = {
        os.path.basename(x):x
        for x in glob.glob(f'{f_inputs}/*.csv')
    }
    
    def run_cdm(name,destination_table,rules,ti):
        inputs = coconnect.tools.load_csv(f_input_map,
                                          chunksize=None)
        
        cdm = coconnect.cdm.CommonDataModel(inputs=inputs,
                                            format_level=2,
                                            output_folder=f"{f_output_folder}/{destination_table}/{name}/")
        
        obj = coconnect.cdm.get_cdm_class(destination_table)()
        obj.set_name(name)
        obj.rules = rules
        obj.define = lambda self : coconnect.tools.apply_rules(self)
        cdm.add(obj)
        cdm.process()

        return { name:cdm.logs['meta']['output_files']}
    
    def merge_tables(files, **kwargs):
        files = files.replace("'",'"')
        files = json.loads(files)

        #invert data
        objects = {}
        inputs = []
        for jobs in files:
            job_name = list(jobs.keys())[0]
            jobs = jobs[job_name]
            for destination_table,fname in jobs.items():
                if destination_table not in objects:
                    objects[destination_table] = {}
                objects[destination_table][job_name] = fname
                inputs.append(fname)

        inputs = coconnect.tools.load_csv(inputs,
                                          sep='\t',
                                          na_values=[''],
                                          chunksize=100)

        cdm = coconnect.cdm.CommonDataModel(inputs=inputs,
                                            format_level=0,
                                            output_folder=f"{f_output_folder}/"
        )
        
        for destination_table,objs in objects.items():
            for name,fname in objs.items():
                obj = coconnect.cdm.get_cdm_class(destination_table)()
                obj.set_name(name)
                obj.fname = fname
                obj.define =  lambda x : coconnect.tools.load_from_file(x)
                cdm.add(obj)
        cdm.process()
        return cdm.logs['meta']['output_files']


    def mask_tables(f_in,destination_table,**kwargs):
        f_in = json.loads(f_in.replace("'",'"'))
        return f_in[destination_table]
    
    
    def coconnect_etl():

        config = json.load(open(f_rules))

        #extract
        with TaskGroup(group_id='extract') as extract:
                for name,f_in in f_input_map.items():
                    salt = DummyOperator(task_id=f"salt_{name}")
            
        
        #transform
        with TaskGroup(group_id='transform') as transform:

            #finish = DummyOperator(task_id=f"finish_merge")

            merge_tasks = {}
            for destination_table,rules_set in config['cdm'].items():
                tasks = {}
                for name,rules in rules_set.items():
                    task_id = re.sub("[^a-zA-Z0-9 ]+", "", name).replace(" ","_")
                    run = PythonOperator(task_id=task_id,
                                         python_callable=run_cdm,
                                         op_kwargs={
                                             'destination_table':destination_table,
                                             'name':name,
                                             'rules':rules
                                         },
                                         retries=0)
                    tasks[f"transform.{task_id}"] = run

                tasks_str = '["'+'","'.join(list(tasks.keys()))+'"]'
                merge = PythonOperator(task_id=f'merge_{destination_table}',
                                       python_callable=merge_tables,
                                       op_kwargs={
                                           'files':'{{ ti.xcom_pull(task_ids='+tasks_str+') }}'
                                       })

                merge_tasks[destination_table] = merge
                
                for run in tasks.values():
                    run >> merge 
                    

            mask_tasks = {}
            for destination_table in config['cdm'].keys():
                mask = PythonOperator(task_id=f'mask_{destination_table}',
                                      python_callable=mask_tables,
                                      op_kwargs = {
                                          "f_in":'{{ ti.xcom_pull(task_ids="transform.merge_'+destination_table+'") }}',
                                          "destination_table":destination_table
                                      })
                mask_tasks[destination_table] = mask

                
            for destination_table in mask_tasks.keys():
                if destination_table == 'person': 
                    merge_tasks[destination_table] >> mask_tasks[destination_table]
                else:
                    [merge_tasks[destination_table] , mask_tasks['person']] >> mask_tasks[destination_table]

        # load
        with TaskGroup(group_id='load') as load:
            for destination_table in config['cdm'].keys():

                command = f'echo datasettool2 delete-all-rows {destination_table} --database=bclink'
                delete = BashOperator(task_id=f'delete_{destination_table}',
                                      bash_command=command)

                fname = '{{ ti.xcom_pull(task_ids="transform.mask_'+destination_table+'") }} '
                command = f'echo dataset_tool --load --table={destination_table} --user=data --data_file={fname} --support  --bcqueue --bcqueue-res-path=./logs/{destination_table}  bclink'
                
                upload = BashOperator(task_id=f'upload_{destination_table}',
                                      bash_command=command)

                delete >> upload
                
        extract >> transform >> load 
        
            
    d = dag(dag_name,
            default_args=default_args,
            schedule_interval=timedelta(**f_schedule),
            start_date=days_ago(2),
            tags=[dag_name,'dataset'])(coconnect_etl)()

    return d
