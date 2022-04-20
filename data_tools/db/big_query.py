"""
# Developer: Richard Raphael Banak
# Objective: Functions to help big query actions
# Creation date: 2022-04-01
"""

from google.cloud import bigquery
from google.cloud.exceptions import NotFound

client = bigquery.Client()


# -----------------------------------------------------------------------------
# CRUD ------------------------------------------------------------------------


def run_query(query):
    return client.query(query)


def select(query):
    return client.query(query).result().to_dataframe()


def insert_rows_into_table(table_id, rows_to_insert, show_log=False):
    errors = client.insert_rows_json(table_id, rows_to_insert)
    if errors == []:
        if show_log == True:
            print(f"New rows have been added into table [{table_id}].")
    else:
        raise Exception(
            f"ERROR: Encountered errors while inserting rows into table [{table_id}]: {errors}"
        )


def create_dataset(dataset_id):
    try:
        dataset = bigquery.Dataset(dataset_id)
        client.create_dataset(dataset=dataset, timeout=30)
        print(f"Dataset created: [{dataset_id}]")
    except Exception as e:
        if e.code == 409:
            print(f"Dataset already exists: [{dataset_id}].")
        else:
            raise Exception(e)


def delete_dataset(dataset_id, delete=False):
    if delete == False:
        print(
            f"Pass parameter delete as True on the 'delete_dataset' function to confirm dataset deletion: [{dataset_id}]."
        )
        return

    try:
        client.delete_dataset(dataset_id, delete_contents=True, not_found_ok=True)
        print(f"Dataset deleted: [{dataset_id}]")
    except Exception as e:
        if e.code == 409:
            print(f"Dataset already exists: [{dataset_id}].")
        else:
            raise Exception(e)


def delete_table(table_name, delete=False):
    if delete == False:
        print(
            f"Pass parameter delete as True on the 'delete_dataset' function to confirm dataset deletion: [{table_name}]."
        )
        return

    client.delete_table(table_name, not_found_ok=True)
    print(f"Table deleted: [{table_name}].")


# -----------------------------------------------------------------------------
# Get -------------------------------------------------------------------------


def get_rows_from_query(query):
    return client.query(query).result().total_rows


def get_table_info(dataset_id, table_name):
    if check_table_exists(dataset_id=dataset_id, table_name=table_name):
        return select(
            f"""SELECT * FROM {dataset_id}.INFORMATION_SCHEMA.TABLES where table_name = '{table_name}' """
        )
    else:
        raise Exception(f"Table doesn`t exist: [{table_name}].")


def get_table_ddl(dataset_id, table_name):
    if check_table_exists(dataset_id=dataset_id, table_name=table_name):
        return select(
            f"""SELECT ddl FROM {dataset_id}.INFORMATION_SCHEMA.TABLES where table_name = '{table_name}' """
        ).ddl[0]
    else:
        raise Exception(f"Table doesn`t exist: [{table_name}].")


# -----------------------------------------------------------------------------
# Check -----------------------------------------------------------------------


def check_dataset_exists(dataset_id):
    try:
        client.get_dataset(dataset_id)
        return True
    except NotFound:
        return False


def check_table_exists(table_name):
    try:
        client.get_table(table_name)
        return True
    except NotFound:
        return False
      
