import psycopg2
from sql_queries import *

print("try connect to postgresdb")

# table_drop = "DROP TABLE IF EXISTS songplays"

# table_create = """
#     CREATE TABLE IF NOT EXISTS xxx (
#     )
# """

# create_table_queries = [
    
#     #table_create,
#     fact_event_table_create,    
# ]

# drop_table_queries = [
#     table_drop,
# ]


def drop_tables(cur, conn) -> None:
    """
    Drops each table using the queries in `drop_table_queries` list.
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn) -> None:
    """
    Creates each table using the queries in `create_table_queries` list.
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    - Drops (if exists) and Creates the sparkify database.
    - Establishes connection with the sparkify database and gets
    cursor to it.
    - Drops all the tables.
    - Creates all tables needed.
    - Finally, closes the connection.
    """
    conn = psycopg2.connect(
        host="localhost",
        database="postgres",
        user="postgres",
        password="postgres")
        #"host=127.0.0.1:8080 dbname=postgres user=postgres password=postgres"
    
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()
    print("connection close")


if __name__ == "__main__":
    main()
