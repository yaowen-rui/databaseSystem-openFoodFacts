import psycopg2
import csv
import pandas as pd

def insert_matches_toDB():
    path = "/Users/rui/Downloads/DatabaseSystem/dataset-integrate/matches.csv"
    df = pd.read_csv(path)
    try:
        conn = psycopg2.connect(
            dbname="database_project", user="user", password="password", host="127.0.0.1", port=54321
        )
        cur = conn.cursor()
        table_name = "matches"

        drop_table_query = f"DROP TABLE IF EXISTS {table_name};"
        cur.execute(drop_table_query)
        conn.commit()
        print(f"Table '{table_name}' dropped if it existed.")

        create_table_query = f"""
        CREATE TABLE {table_name} (
            {df.columns[0]} VARCHAR(255),
            {df.columns[1]} INTEGER[]
            );
        """
        cur.execute(create_table_query)
        conn.commit()
        print(f"Table '{table_name}' created successfully.")

        # Insert data into the table
        for _, row in df.iterrows():
            row_values = row.tolist()
            # Ensure the second column (list of integers) is formatted correctly
            try:
                row_values[1] = '{' + ','.join(map(str, eval(row_values[1]))) + '}'  # Format for PostgreSQL INTEGER[]
            except Exception as format_error:
                print(f"Error formatting row {row_values[1]}: {format_error}")
            
            insert_query = f"""
            INSERT INTO {table_name} ({', '.join(df.columns)})
            VALUES ({', '.join(['%s' for _ in df.columns])});
            """
            cur.execute(insert_query, tuple(row_values))
        conn.commit()
        print(f"Data inserted into table '{table_name}' successfully.")

    except Exception as e:
        print("An error occurred:", e)
    finally:
        if conn:
            cur.close()
            conn.close()

# access the container command:
# docker exec -it database_project bash

# Once inside the container, run the pg_dump command:
# pg_dump -U user -d database_project -F c -f db_group1_dump.sql

#After generating the dump, copy it from the container to your host machine command:
# docker cp database_project:/db_group1_dump.sql ./db_group1_dump.sql
