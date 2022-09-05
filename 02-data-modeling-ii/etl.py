from cassandra.cluster import Cluster
from collections import Counter

from typing import List
import glob
import json
import os


table_drop = "DROP TABLE events"


table_create = """
    CREATE TABLE IF NOT EXISTS events
    (        
        login_name varchar,
        numberofEvent int,
        PRIMARY KEY (login_name,numberofEvent)             
    );
     

"""

create_table_queries = [
    table_create,
]
drop_table_queries = [
    table_drop,
]

def drop_tables(session):
    for query in drop_table_queries:
        try:
            rows = session.execute(query)
        except Exception as e:
            print(e)


def create_tables(session):
    for query in create_table_queries:
        try:
            session.execute(query)
        except Exception as e:
            print(e)

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

def process(session, filepath):
    # Get list of files from filepath
    all_files = get_files(filepath)
    
    list_actor_login = []

    for datafile in all_files:
        
        with open(datafile, "r") as f:
            data = json.loads(f.read())
            
            for each in data:                   
                
                #test number of count on each actor_login                
                list_actor_login.append(each["actor"]["login"]) 
    
    count_actorlogin_event = Counter(list_actor_login)    
    # actor_login_key = count_actorlogin_event.keys()
    # number_exist_each_actor_id = count_actorlogin_event.values()
        
    for key, value in count_actorlogin_event.items():
        # Insert data into tables here
        record_to_insert_events = ( 
            key,
            value           
            )

        query = """
        INSERT INTO events (login_name, numberofEvent) 
        VALUES  (%s, %s);
        """
        session.execute(query,record_to_insert_events)
    
    

def main():
    cluster = Cluster(['127.0.0.1'])
    session = cluster.connect()

    # Create keyspace
    try:
        session.execute(
            """
            CREATE KEYSPACE IF NOT EXISTS github_events
            WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }
            """
        )
    except Exception as e:
        print(e)

    # Set keyspace
    try:
        session.set_keyspace("github_events")
    except Exception as e:
        print(e)

    drop_tables(session)
    create_tables(session)

    process(session, filepath="../data")
    # insert_sample_data(session)

    # Select data in Cassandra and print them to stdout
    query = """
    SELECT * from events
    WHERE numberofEvent > 1 ALLOW FILTERING;
    
    """
    try:
        rows = session.execute(query)
    except Exception as e:
        print(e)

    for row in rows:
        print(row)


if __name__ == "__main__":
    main()
