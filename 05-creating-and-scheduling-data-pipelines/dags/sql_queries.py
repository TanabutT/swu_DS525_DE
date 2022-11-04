# DROP TABLES
# actualy cascade no need to use if drop sql queries order is correct
fact_event_table_drop = "DROP TABLE IF EXISTS fact_event;"
dim_actor_table_drop = "DROP TABLE IF EXISTS dim_actor CASCADE;"
dim_repo_table_drop = "DROP TABLE IF EXISTS dim_repo CASCADE;"
dim_payload_push_table_drop = "DROP TABLE IF EXISTS dim_payload_push CASCADE;"
dim_org_table_drop = "DROP TABLE IF EXISTS dim_org CASCADE;"

# CREATE TABLES

dim_actor_table_create = ("""CREATE TABLE IF NOT EXISTS dim_actor 
 (
    actor_id INT,
    actor_login varchar,
    actor_display_login varchar,
    actor_gravatar_id varchar,
    actor_url varchar,
    actor_avatar_url varchar, 
    PRIMARY KEY (actor_id)
    
);
""")

dim_repo_table_create = ("""CREATE TABLE IF NOT EXISTS dim_repo 
 (
    repo_id INT,
    repo_name varchar,
    repo_url varchar, 
    PRIMARY KEY (repo_id)
    
);
""")

dim_payload_push_table_create = ("""CREATE TABLE IF NOT EXISTS dim_payload_push 
 (
    push_id BIGINT, 
    size INT,
    distinct_size INT,
    ref varchar,
    head varchar,
    before_code varchar,
    commits varchar,
    PRIMARY KEY (push_id)
);
""")

dim_org_table_create = ("""CREATE TABLE IF NOT EXISTS dim_org 
 (
    org_id INT,
    org_login varchar,
    org_gravatar_id varchar,
    org_url varchar,
    org_avatar_url varchar,
    PRIMARY KEY (org_id)
);
""")


fact_event_table_create = ("""CREATE TABLE IF NOT EXISTS fact_event
 (
    --id INT NOT NULL AUTO_INCREMENT,
    event_id BIGINT NOT NULL,     
    event_type varchar,
    actor_id INT,
    repo_id INT,
    payload_action varchar, -- [None (is 'push'), 'start', 'created', 'published', 'closed']
    payload_push_id BIGINT,
    pulic BOOLEAN,
    create_at varchar, 
    org_id INT, 
    event_time timestamp NOT NULL,
    PRIMARY KEY (event_id),
    CONSTRAINT FK_actor_event FOREIGN KEY (actor_id) REFERENCES dim_actor(actor_id),
    CONSTRAINT FK_repo_event FOREIGN KEY (repo_id) REFERENCES dim_repo(repo_id),
    CONSTRAINT FK_payload_push_event FOREIGN KEY (payload_push_id) REFERENCES dim_payload_push(push_id),
    CONSTRAINT FK_org_event FOREIGN KEY (org_id) REFERENCES dim_org(org_id)
);

 """)




# INSERT RECORDS

dim_actor_table_insert = ("""INSERT INTO dim_actor
 ( actor_id ,actor_login , actor_display_login , actor_gravatar_id , actor_url ,actor_avatar_url )
 VALUES (%s, %s, %s, %s, %s, %s)
 ON CONFLICT (actor_id) DO NOTHING;
""")

dim_repo_table_insert = ("""INSERT INTO dim_repo
 ( repo_id ,repo_name , repo_url )
 VALUES (%s, %s, %s)
 ON CONFLICT (repo_id) DO NOTHING;
""")


dim_payload_push_table_insert = ("""INSERT INTO dim_payload_push 
 (  push_id ,    size,     distinct_size ,    ref ,    head ,  before_code ,    commits  )
 VALUES (%s, %s, %s, %s, %s, %s, %s)
 ON CONFLICT (push_id) DO NOTHING;
""")

dim_org_table_insert = ("""INSERT INTO dim_org 
 (    org_id ,    org_login ,    org_gravatar_id ,    org_url ,    org_avatar_url     )
 VALUES (%s, %s, %s, %s, %s)
 ON CONFLICT (org_id) DO NOTHING;
""")

fact_event_table_insert = ("""INSERT INTO fact_event
 (event_id , event_type, actor_id,  repo_id,   payload_action, payload_push_id,  pulic,  create_at,   org_id,  event_time )
 VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
 ON CONFLICT (event_id) DO NOTHING;
 """)




# QUERY LISTS

create_table_queries = [
    dim_actor_table_create, 
    dim_repo_table_create, 
    dim_payload_push_table_create, 
    dim_org_table_create,
    fact_event_table_create
    ]
    # order in drop table should drop fact first all constrain will free and no need to use casecade
drop_table_queries = [
    fact_event_table_drop,
    dim_actor_table_drop, 
    dim_repo_table_drop, 
    dim_payload_push_table_drop, 
    dim_org_table_drop
    ]

