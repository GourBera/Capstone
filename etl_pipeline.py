import os
import psycopg2
import configparser
import pandas as pd
from s3_access import upload_file
from sql_queries import copy_table_queries, insert_table_queries


# Extract, process data and upload them into s3 bucket
def extract_data():
    # Read config file
    config = configparser.ConfigParser()
    config.read('db_config.cfg')
    directory = config['FILE_PATH']['INPUT']

    # Walk through the directory and read data into pandas DataFrame
    df_info = dict()
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith(".csv"):
                df_name = f"df_{file.split('.')[0]}".lower()
                df_path = os.path.join(directory, file)
                df = pd.read_csv(df_path)
                df[df_name] = 'Yes'

                # Data issue - make column name consistent
                df.columns = [col.replace('_-_', '_')
                                  .replace('-', '').replace('/', '_')
                                  .lower() for col in list(df)]
                df_info[df_name] = df

    # Merging all DataFrame
    result = pd.concat(df_info.values())

    # Data issue - make data type consistent
    col_list = list(result)
    obj_col_type = ['area_name', 'df_auto_theft', 'df_complaints_against_police', 'df_property_stolen_and_recovered',
                    'df_serious_fraud', 'df_victims_of_rape', 'group_name', 'sub_group_name']

    for col in obj_col_type:
        col_list.remove(col)

    result[col_list] = result[col_list].fillna(0.0).astype(int)

    # Save process file into directory
    csv_out_path = os.path.join(directory, "output.csv")
    result.to_csv(csv_out_path, index=False)

    # Upload processed file to S3 Bucket
    upload_file(csv_out_path, config['FILE_PATH']['BUCKET'])


# Function to load data into staging table
def load_staging_tables(cur, conn):
    """
    :param cur: db cursor
    :param conn: db connection
    :return:
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()
    print('Load into staging table completed')


# Function to insert data into table
def insert_tables(cur, conn):
    """
    :param cur: db cursor
    :param conn: db connection
    :return:
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()
        print("Insert into Table completed")


# Main Function to execute  load_staging_tables and insert_tables
def main():
    config = configparser.ConfigParser()
    config.read('db_config.cfg')

    conn = psycopg2.connect(
        "host={} dbname={} user={} password={} port={}".format(
            *config['CLUSTER'].values())
    )
    cur = conn.cursor()

    extract_data()
    print('Extract and upload to s3 completed')
    insert_tables(cur, conn)

    # Close db connection
    conn.close()


if __name__ == "__main__":
    main()
