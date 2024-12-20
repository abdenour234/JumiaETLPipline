import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError

def load(df, db_user='postgres', db_password='oujdaoujda', db_host='localhost', db_port='5432', db_name='Jumia', table_name= 'products', if_exists='append'):
    """
    Load data into a PostgreSQL database table and remove duplicates based on the 'name' column.

    Parameters:
    - df: pandas DataFrame, the data to load
    - db_user: str, database username
    - db_password: str, database password
    - db_host: str, database host
    - db_port: str, database port
    - db_name: str, database name
    - table_name: str, target table name in the database
    - if_exists: str, behavior if the table exists ('fail', 'replace', 'append')

    Returns:
    - int: 1 if successful, 0 if an error occurs
    """
    try:
        # Create the database connection string
        connection_string = f'postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}'
        
        # Create a SQLAlchemy engine
        engine = create_engine(connection_string)
        
        # Push the DataFrame to the PostgreSQL database
        df.to_sql(table_name, engine, if_exists=if_exists, index=False)

        # Remove duplicate rows based on the 'name' column
        with engine.connect() as connection:
            delete_duplicates_query = f"""
            DELETE FROM {table_name} t1
            USING {table_name} t2
            WHERE t1.ctid < t2.ctid
            AND t1."Name" = t2."Name";
            """
            connection.execute(delete_duplicates_query)

        print(f"Data pushed to {table_name} in the {db_name} database successfully.")
        return 1
    except SQLAlchemyError as e:
        print(f"An error occurred: {e}")
        return 0

