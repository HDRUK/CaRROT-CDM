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
import carrot 

default_dir = os.path.dirname(carrot.__file__)+"/data/test/"


f_debug_level = 3
carrot.params['debug_level'] = f_debug_level

f_schedule = {'days': 5}

default_args = {
    'owner': 'airflow',
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
}

from airflow.api.client.local_client import Client

def get_headers(token):
    return {
        "User-Agent": "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.119 Safari/537.36",
        "Content-type": "application/json",
        "charset": "utf-8",
        "Authorization": f"Token {token}"
    }

def get_dag_folder():
    airflow_home = os.environ.get('AIRFLOW_HOME')

    config = configparser.RawConfigParser()
    config.read(f'{airflow_home}/airflow.cfg')
    dags_folder = dict(config.items('core'))['dags_folder']
    return dags_folder

def create_template(report_name,report_id):

    f_name = re.sub("[^a-zA-Z0-9 ]+", "", report_name).replace(" ","_")
    f_name = f"{f_name}_{report_id}"
    
    airflow_home = os.environ.get('AIRFLOW_HOME')

    workdir = f"{airflow_home}/data/{f_name}"
    os.makedirs(workdir,exist_ok=True)
    
    config = configparser.RawConfigParser()
    config.read(f'{airflow_home}/airflow.cfg')
    dags_folder = dict(config.items('core'))['dags_folder']
    
    _dir = f"{dags_folder}/etl_dags"
    os.makedirs(_dir,exist_ok=True)

    
    template = f'''
from carrot.workflows.airflow import create_etl_dag
import json
import carrot

dag = create_etl_dag("{f_name}",
                      report_name="{report_name}",
                      report_id="{report_id}",
                      workdir="{workdir}")

'''


    f_name = f"{_dir}/{f_name}.py"
    with open(f_name,"w") as f:
        f.write(template)

    return f_name


def get_etl_dags():
    from airflow.models import DagBag
    from airflow.utils.cli import  process_subdir
    
    _dir = get_dag_folder()
    
    dagbag = DagBag(process_subdir(_dir))

    tags = ['etl','scanreport']
    return [ dag.dag_id
             for dag in dagbag.dags.values()
             if all([tag in dag.tags for tag in tags])
    ]
    
def trigger_etl_dags():
    c = Client(None, None)
    dag_ids = get_etl_dags()
    for dag_id in dag_ids:
        c.trigger_dag(dag_id=dag_id)


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
     #max_active_runs=2,
     #max_active_tasks=6,
     start_date=days_ago(2),
     tags=['manager'])
def etl():

    url,token = get_url_and_token()
    _id = '{{ dag_run.conf["report_id"] }}'

    def get_info_from_scan_report(_id):
        _url = f'{url}/api/scanreports/{_id}'
        headers = get_headers(token)
        response = requests.get(_url,headers=headers)
        info = response.json()

        dataset = info['dataset']
        created = info['updated_at']

        _dir = get_dir()
        _dir = f'{_dir}/{dataset}/{created}'
        os.makedirs(_dir,exist_ok=True)

        info['workdir'] = _dir
        
        return info


    def get_json(_id,previous_step,**kwargs):
        _url = f'{url}/api/json/?id={_id}'
        headers = get_headers(token)
        response = requests.get(_url,headers=headers).json()[0]
        print (json.dumps(response,indent=6))

        ti = kwargs['ti']
        info = ti.xcom_pull(previous_step)
        _dir = info['workdir']
        
        f_name = f"{_dir}/rules.json"
        with open(f_name,'w') as outfile:
            json.dump(response, outfile, indent=6)
    
        return f_name
    

    start = PythonOperator(
        task_id=f"start",
        python_callable=get_info_from_scan_report,
        op_kwargs={
            '_id': _id,
        }
    )
    
    download_json = PythonOperator(
        task_id=f"get_json",
        python_callable=get_json,
        op_kwargs={
            '_id': _id,
            'previous_step':'start'
        }
    )

    nevents = 1000
    output_directory = '{{ ti.xcom_pull("start")["workdir"] }}/input_data/'
    generate_synthetic = BashOperator(
        task_id=f"generate_synthetic",
        bash_command=f'carrot --carrot-mapper-token {token} --carrot-mapper-url {url} generate \
        synthetic from-carrot-mapper  --number-of-events {nevents} --report-id {_id} \
        --output-directory \"{output_directory}\" '
    )
 
    output_directory = '{{ ti.xcom_pull("start")["workdir"] }}/output_data/'
    inputs = '{{ ti.xcom_pull("start")["workdir"] }}/input_data/'
    rules = '{{ ti.xcom_pull("get_json") }}'
    
    run_carrot_tools = BashOperator(
        task_id=f"run_carrot_tools",
        bash_command=f'carrot run map --output-folder \"{output_directory}\" --rules \"{rules}\" \
        \"{inputs}\" '
    )

    create_dag = PythonOperator(
        task_id=f"make_dag",
         python_callable=make_dag
    )
    
    start >> create_dag#[ download_json, generate_synthetic] >> create_dag
    #run_carrot_tools
    
    
    # create_dag = PythonOperator(
    #     task_id=f"make_dag",
    #     python_callable=make_dag
    # )

    # decide = BranchPythonOperator(
    #     task_id=f"decision_trigger",
    #     python_callable=decide_trigger
    # )
    
    # trigger_dag = TriggerDagRunOperator(
    #     task_id=f"trigger_dag",
    #     trigger_dag_id='{{ ti.xcom_pull("make_dag") }}',
    #     wait_for_completion=True,
    #     retries=0
    # )

    # finish = BashOperator(
    #     task_id=f"finish",
    #     bash_command='echo done!'
    # )

    
    #start #>> download_json >> run_synthetic >> create_dag >> decide
    #trigger_dag >> finish
    #decide >> [ trigger_dag, finish] 
    

def get_url_and_token(ti=None):

    url = token = None

    #try:
    url = Variable.get("carrot_mapper_url")
    token = Variable.get("carrot_mapper_token")
    #except Exception as err:
    #    pass

    if ti is not None:
        dag_run = ti['dag_run']
        if "url" in dag_run.conf and "token" in dag_run.conf:
            url = dag_run.conf["url"]
            token = dag_run.conf["token"]
        else:
            raise Exception("no token or url given")

    return url,token

    
def get_reports():
    url,token = get_url_and_token()
    headers = get_headers(token)
    _url = f"{url}/api/scanreports/"
    response = requests.get(_url,headers=headers).json()
    reports = {}
    for report in response:
        if report['hidden'] == True:
            continue
        reports[str(report['id'])] = report['dataset']
        
    return reports


def carrot_report_getters():


    url,token = get_url_and_token()
    reports = get_reports()
    task_ids = []
    for _id,(name,date) in reports.items():
        name = re.sub("[^a-zA-Z0-9 ]+", "", name).replace(" ","_")
        task_id = f"{name}_{_id}"


def create_etl_dag(dag_name,report_name=None,report_id=None,workdir="./",schedule=None):

    url,token = get_url_and_token()
    
    def check_data(workdir):
        f_json = f"{workdir}/rules.json"
        f_data = f"{workdir}/data/"

        json_exists = os.path.exists(f_json)
        data_exists = os.path.isdir(f_data)
        
        if not json_exists or not data_exists:
            return "get_data"
        #elif json_exists and not data_exists:
        #    return "get_synthetic_data"
        #elif not json_exists and data_exists:
        #    return "get_json"
        else:
            return "transform"

    def get_json(_id,workdir):
        _url = f'{url}/api/json/?id={_id}'
        headers = get_headers(token)
        response = requests.get(_url,headers=headers).json()[0]
        print (json.dumps(response,indent=6))

        dataset = response['metadata']['dataset']
        created = response['metadata']['date_created']
        os.makedirs(workdir,exist_ok=True)

        f_name = f"{workdir}/rules.json"
        with open(f_name,'w') as outfile:
            json.dump(response, outfile, indent=6)
            
        return f_name

        
    
    with DAG(
            dag_name,
            default_args=default_args,
            description=f'ETL for {report_name} with id {report_id}',
            schedule_interval=schedule,
            start_date=days_ago(2),
            tags=['scanreport','etl',report_name,report_id],
    ) as dag:


        do_extract = BranchPythonOperator(
           task_id=f"do_extract",
           python_callable=check_data,
           op_kwargs={
               'workdir':workdir
           }
        )

        get_data = DummyOperator(task_id=f"get_data")
        get_json = PythonOperator(
            task_id=f"get_json",
            python_callable=get_json,
            op_kwargs={
                '_id':report_id,
                'workdir':workdir
            }
        )

        nevents = 1000
        output_directory = f'{workdir}/data/'
        get_synthetic_data = BashOperator(
            task_id=f"get_synthetic_data",
            bash_command=f'carrot --carrot-mapper-token {token} --carrot-mapper-url {url} generate \
            synthetic from-carrot-mapper  --number-of-events {nevents} --report-id {report_id} \
            --output-directory \"{output_directory}\" '
        )
 
        rules = f"{workdir}/rules.json"
        data = f"{workdir}/data/"
        transform = BashOperator(
            task_id="transform",
            trigger_rule='none_failed_min_one_success',
            bash_command=f'carrot run map --rules \"{rules}\" \"{data}\"'
        )

                
        do_extract >> [transform,get_data]

        get_data >> [get_json,get_synthetic_data] >> transform
        
        return dag
    

def create_dag(dag_name,f_inputs,f_output_folder,f_rules,f_schedule={'weeks':4}):
    
    f_input_map = {
        os.path.basename(x):x
        for x in glob.glob(f'{f_inputs}/*.csv')
    }
    
    def run_cdm(name,destination_table,rules,ti):
        inputs = carrot.tools.load_csv(f_input_map,
                                          chunksize=None)
        
        cdm = carrot.cdm.CommonDataModel(inputs=inputs,
                                            format_level=2,
                                            output_folder=f"{f_output_folder}/{destination_table}/{name}/")
        
        obj = carrot.cdm.get_cdm_class(destination_table)()
        obj.set_name(name)
        obj.define = lambda x,rules=rules : carrot.tools.apply_rules(x,rules,inputs=cdm.inputs)
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

        inputs = carrot.tools.load_csv(inputs,
                                          sep='\t',
                                          na_values=[''],
                                          chunksize=100)

        cdm = carrot.cdm.CommonDataModel(inputs=inputs,
                                            format_level=0,
                                            output_folder=f"{f_output_folder}/"
        )
        
        for destination_table,objs in objects.items():
            for name,fname in objs.items():
                obj = carrot.cdm.get_cdm_class(destination_table)()
                obj.set_name(name)
                obj.fname = fname
                obj.define =  lambda x : carrot.tools.load_from_file(x)
                cdm.add(obj)
        cdm.process()
        return cdm.logs['meta']['output_files']


    def mask_tables(f_in,destination_table,**kwargs):
        f_in = json.loads(f_in.replace("'",'"'))
        return f_in[destination_table]
    
    
    def carrot_etl():

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
                    #dedup = DummyOperator(task_id='dedup_calc_and_check')
                    #mask = DummyOperator(task_id='handle_masked_ids')
                    merge_tasks[destination_table]  >> mask_tasks[destination_table]
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
            mask = DummyOperator(task_id='upload_masked_ids')
                
        extract >> transform >> load 
        
            
    d = dag(dag_name,
            default_args=default_args,
            schedule_interval=timedelta(**f_schedule),
            start_date=days_ago(2),
            tags=[dag_name,'dataset'])(carrot_etl)()

    return d
