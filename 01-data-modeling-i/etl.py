import glob
import json
import os
import logging
from typing import List
from datetime import datetime
from sql_queries import *

import psycopg2


# table_insert = """
#     INSERT INTO users (
#         xxx
#     ) VALUES (%s)
#     ON CONFLICT (xxx) DO NOTHING
# """


def get_files(filepath: str) -> List[str]:
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


def process(cur, conn, filepath):
    # Get list of files from filepath
    all_files = get_files(filepath)

    # get datetime
    curr_dt = datetime.now()

    for datafile in all_files:
        with open(datafile, "r") as f:
            data = json.loads(f.read())
            for each in data:
                # Print some sample data
                # print(each["id"], each["type"], each["actor"]["login"])
                
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

                # payload_action = None
                # if "push_id" in list(each['payload'].keys()):
                #     payload_action          = each["payload"]["push_id"]
                #     payload_size            = each["payload"]["size"]       
                #     payload_distint_size    = each["payload"]["distinct_size"]
                #     payload_ref             = each["payload"]["ref"]        
                #     payload_head            = each["payload"]["head"]       
                #     payload_before          = each["payload"]["before"]
                #     payload_commits         = each["payload"]["commits"]
                #     print('this row has push_id')     
                # elif "action" in list(each["payload"].keys()):
                #     payload_action = each["payload"]["action"]
                #     payload_size            = None
                #     payload_distint_size    = None
                #     payload_ref             = None
                #     payload_head            = None
                #     payload_before          = None
                #     payload_commits         = None
                #     print('payload_action--no push_id')                     
                # else:
                #     print("payload push_id not found - payload action not found") 
                
                #if none skip row how????  here the answer below !!!!! (beware of if condition below will be AND conditon from here)
            for each in data:
                if each.get("payload").get("push_id") == None:
                    continue
                # print(each.get("payload").get("push_id"))  

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

            #if none skip row how????  here the answer below !!!!!
            # count_org_exist = 0
            for each in data:                
                if each.get("org") == None:
                    continue
                # count_org_exist += 1   # same as count_org_exist = count_org_exist + 1
                # print(count_org_exist)
                # print(each.get("org").get("id"))
                record_to_insert_dim_org = (
                    each.get("org").get("id"), 
                    each.get("org").get("login"),
                    each.get("org").get("gravatar_id"),
                    each.get("org").get("url"),
                    each.get("org").get("avatar_url")
                    )    

                cur.execute(dim_org_table_insert, record_to_insert_dim_org)
                conn.commit()


            for each in data:                
                org_id_insert = None
                if each.get("org") == None:
                    pass
                else: 
                    org_id_insert = each.get("org").get("id")
                
                record_to_insert_fact_table = (
                    each.get("id"), # event_id , 
                    each.get("type"), # event_type, 
                    each.get("actor").get("id"), # actor_id,  
                    each.get("repo").get("id"), # repo_id,   
                    each.get("payload").get("action"), # payload_action, 
                    each.get("payload").get("push_id"), # payload_push_id,  
                    each.get("public"), # public,  
                    each.get("created_at"), # create_at,   
                    org_id_insert, # org_id,  
                    curr_dt # event_time -- datetime timestamp
                    )
                # print("payload org_id: ", org_id_insert )   
                # print("Current datetime: ", curr_dt)     

                cur.execute(fact_event_table_insert, record_to_insert_fact_table)
                conn.commit()
                

def main():
    # conn = psycopg2.connect(
    #     "host=127.0.0.1 dbname=postgres user=postgres password=postgres"
    # )
    conn = psycopg2.connect(
        host="localhost",
        database="postgres",
        user="postgres",
        password="postgres")
    logging.warning('connected postgresdb')

    cur = conn.cursor()

    process(cur, conn, filepath="../data")
    logging.warning('wrote to db')

    conn.close()
    logging.warning('disconnect postgresdb--closed')



if __name__ == "__main__":
    main()
