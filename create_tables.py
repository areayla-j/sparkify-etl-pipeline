import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def create_database():
    """
    - Creates and connects to the sparkifydb
    - Returns the connection and cursor to sparkifydb
    """
    
    # connect to default database
    conn = psycopg2.connect("host=127.0.0.1 dbname=studentdb user=student password=student")
    conn.set_session(autocommit=True)
    cur = conn.cursor()
    
    # create sparkify database with UTF8 encoding
    cur.execute("DROP DATABASE IF EXISTS sparkifydb")
    cur.execute("CREATE DATABASE sparkifydb WITH ENCODING 'utf8' TEMPLATE template0")

    # close connection to default database
    conn.close()    
    
    # connect to sparkify database
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()
    
    return cur, conn


def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()
        print(f"Dropped table with query:\n{query}")

def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()
        print(f"Created table with query:\n{query}")


def main():
    """
    - Drops (if exists) and Creates the sparkify database. 
    - Establishes connection with the sparkify database and gets cursor to it.  
    - Drops all the tables.  
    - Creates all tables needed. 
    - Finally, closes the connection. 
    """
    try:
        cur, conn = create_database()
        print("✅ Connected to sparkifydb")
    except Exception as e:
        print(f"❌ Error creating or connecting to database: {e}")
        return

    try:
        drop_tables(cur, conn)
        print("🗑️ Tables dropped.")
    except Exception as e:
        print(f"❌ Error dropping tables: {e}")

    try:
        create_tables(cur, conn)
        print("🛠️ Tables created.")
    except Exception as e:
        print(f"❌ Error creating tables: {e}")
    
    conn.close()
    print("🔒 Connection closed.")


if __name__ == "__main__":
    main()
  