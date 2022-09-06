# Building a Data Warehouse

## Getting Started 
* go to aws console - Redshift 
* Provision (create) cluster

- AQUA (Advanced Query Accelerator) turn off
- Node type = ra3.xplus  - the smallest
- Number of node = 1
  - awsuser
  - set password
- click Associate IAM roles select [x]LabRole and click Associate IAM roles
- Additional configurations deselect [ ]Use defaults

```sh
python -m venv ENV
source ENV/bin/activate
pip install -r requirements.txt
```

## Getting Started window (with git bash via vs code)

```sh
CREATE TABLE IF NOT EXISTS github_event (
  id primary key,
  type text,
  actor_login text,
  repo_name text,
  created_at text
  
)
```


## xxx

```sh
docker-compose up
```

To shutdown, press Ctrl+C and run:

```sh
docker-compose down
```

To create table in Cassandra after docker compose up
check if in venv then run :


```sh
python .\etl.py 
```

## Example query from cassandra keyspace
![er](./example_query.jpg)
