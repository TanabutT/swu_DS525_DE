from typing import List
import os
import glob
import json
from sql_queries import *


from airflow import DAG
from airflow.utils import timezone
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook



def _get_files(filepath: str) -> List[str]:
    """
    Description: This function is responsible for listing the files in a directory
    """

    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, "*.json"))
        for f in files:
            all_files.append(os.path.abspath(f))

    num_files = len(all_files)
    print(f"{num_files} files found in {filepath}")

    return all_files

def _create_tables():
    """
    Creates each table using the queries in `create_table_queries` list.
    """
    hook = PostgresHook(postgres_conn_id="my_postgres")
    conn = hook.get_conn()
    cur = conn.cursor()

    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def _process(**context):
    
    hook = PostgresHook(postgres_conn_id="my_postgres")
    conn = hook.get_conn()
    cur = conn.cursor()

    ti = context["ti"]

    # get datetime
    curr_dt = context["ds"]

    # Get list of files from filepath
    all_files = ti.xcom_pull(task_ids="get_files", key="return_value")
    # all_files = get_files(filepath)
    print("all_files : ", len(all_files))
    print("curr_dt : ", curr_dt)

    print("ti : ", ti)


    

    for datafile in all_files:
        with open(datafile, "r") as f:
            data = json.loads(f.read())
            for each in data:
                               
                # Insert data into tables here

                record_to_insert_dim_actor = (
                    each["actor"]["id"], 
                    each["actor"]["login"],
                    each["actor"]["display_login"],
                    each["actor"]["gravatar_id"],
                    each["actor"]["url"],
                    each["actor"]["avatar_url"]
                    )

                record_to_insert_dim_repo = (
                    each["repo"]["id"], 
                    each["repo"]["name"],
                    each["repo"]["url"]
                    )

                cur.execute(dim_actor_table_insert, record_to_insert_dim_actor)
                cur.execute(dim_repo_table_insert, record_to_insert_dim_repo)                
                conn.commit() 
        
                if each.get("payload").get("push_id") != None:                
                    record_to_insert_dim_payload_push = ( 
                        each["payload"].get("push_id"),                    
                        each["payload"].get("size", None),
                        each["payload"].get("distinct_size", None),
                        each["payload"].get("ref", None),
                        each["payload"].get("head", None),
                        each["payload"].get("before", None),
                        str(each["payload"].get("commits", None))
                    )

                    cur.execute(dim_payload_push_table_insert, record_to_insert_dim_payload_push)
                    conn.commit()   

                           
                if each.get("org") != None:                                                                           
                    record_to_insert_dim_org = (
                        each.get("org").get("id"),                        
                        each.get("org").get("login"),
                        each.get("org").get("gravatar_id"),
                        each.get("org").get("url"),
                        each.get("org").get("avatar_url")
                        )    
                                 
                    record_to_insert_fact_table = (
                        each.get("id"), # event_id , 
                        each.get("type"), # event_type, 
                        each.get("actor").get("id"), # actor_id,  
                        each.get("repo").get("id"), # repo_id,   
                        each.get("payload").get("action"), # payload_action, 
                        each.get("payload").get("push_id"), # payload_push_id,  
                        each.get("public"), # public,  
                        each.get("created_at"), # create_at,   
                        each.get("org").get("id"), # org_id,
                        curr_dt # event_time -- datetime timestamp
                        )

                    cur.execute(dim_org_table_insert, record_to_insert_dim_org)
                    cur.execute(fact_event_table_insert, record_to_insert_fact_table)
                    conn.commit()
                else:
                    record_to_insert_fact_table = (
                        each.get("id"), # event_id , 
                        each.get("type"), # event_type, 
                        each.get("actor").get("id"), # actor_id,  
                        each.get("repo").get("id"), # repo_id,   
                        each.get("payload").get("action"), # payload_action, 
                        each.get("payload").get("push_id"), # payload_push_id,  
                        each.get("public"), # public,  
                        each.get("created_at"), # create_at,   
                        each.get("org"), # org_id,
                        curr_dt # event_time -- datetime timestamp
                        )
                    cur.execute(fact_event_table_insert, record_to_insert_fact_table)
                    conn.commit()

with DAG (
    "etl",
    start_date=timezone.datetime(2022, 10, 8),
    schedule="@daily", # use cron " * * * * *"
    tags=["workshop"],
    catchup=False,
) as dag:

    get_files = PythonOperator(
        task_id="get_files",
        python_callable=_get_files,
        op_kwargs={
            "filepath" : "/opt/airflow/dags/data",
        }
    )

    create_tables = PythonOperator(
        task_id="create_tables",
        python_callable=_create_tables,
    )

    process = PythonOperator(
        task_id="process",
        python_callable=_process,
    ) 

    get_files >> create_tables >> process