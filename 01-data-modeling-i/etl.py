import glob
import json
import os
import logging
from typing import List
from datetime import datetime
from sql_queries import *
import psycopg2


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

                

def main():
    
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
