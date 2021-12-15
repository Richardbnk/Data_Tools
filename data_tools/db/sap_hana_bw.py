"""
# Developer: Richard Raphael Banak
# Objective: Functions to help ETL and operations on SAP HANA BW
# Creation date: 2020-01-05
"""

from numpy import dtype
from sqlalchemy import String, create_engine
import os
import pandas as pd
from hdbcli import dbapi
from datetime import datetime

from . import tools

sh_user = ''
sh_pass = ''
address = ''
port = ''
encoding = 'utf8'
 
#sh_conn = dbapi.connect(
#    address=address,
#    port=port,
#    user=sh_user,
#    password=sh_pass
#)

engine = create_engine(f'hana://{sh_user}:{sh_pass}@{address}:{port}/?charset={encoding}', echo=False)


def run_query(query):

    with engine.connect() as con:
        con.execute(query)


def run_select(query):
    
    return pd.read_sql(query, con=engine)


def get_sql(sql_path, encoding='latin1'):
    
    sql = open(sql_path, encoding=encoding).read()
    
    return sql


def get_queries_from_sql(sql, separator):
    
    return sql.split(separator)


def remove_comment_from_sql(sql):
    
    queries = get_queries_from_sql(sql, ' ; ')
    for query in queries:
        if "/*" in query and "*/" in query:
            itens = query.split('/*')
            for item in itens:
                if '*/' in item:
                    comment = '/*{}*/'.format(item.split('*/')[0])
                    sql = sql.replace(comment, "")

    queries = get_queries_from_sql(sql, ' ; ')
    for query in queries:
        query_rows = query.split("\n")

        for linha in query_rows:
            if '--' in linha:
                itens = linha.split("--", 1)
                comment = "--" + itens[1]
                sql = sql.replace(comment, "", 1)
    return sql


def run_sql_file(sql_file, separator):
    
    sql = get_sql(sql_file)
    sql = remove_comment_from_sql(sql)
    queries = get_queries_from_sql(sql, separator)

    for query in queries:
        run_query(query)


def run_select_from_sql_file(sql_file):
    
    sql = get_sql(sql_file)
    sql = remove_comment_from_sql(sql)
    run_query(sql)


def table_is_empty(table):
    
    query = f"SELECT TOP 1 * FROM {table}"
    df = run_select(query)
    
    return df.empty


def drop_table(table):

    run_query(f"DROP TABLE {table}")


def clean_table(table, where_condition = None):

    query = f'DELETE FROM {table}'

    if where_condition:
        query = query + f' WHERE {where_condition}'

    run_query(query)


def drop_tables_from_list(table_list):
    
    for table in table_list:
        drop_table(table)


def export_query_result_to_csv(query, filepath='C:\TEMP\Export.csv', separator=';', header=True, 
    index=False, encoding=None, date_format=None, decimal=',', quoting=None, float_format=None):

    df = run_select(query)

    tools.export_dataframe_to_csv(dataframe=df, filepath=filepath,separator=separator, 
        header=header, index=index,encoding=encoding, date_format=date_format, decimal=decimal, 
        quoting=quoting, float_format=float_format)


def export_query_result_to_excel(query, filepath='C:\TEMP\Export.xlsx', encoding=None,  header=True, index=False,
    float_format=None, sheet_name='Sheet1', date_format='YYYY-MM-DD'):

    df = run_select(query)

    tools.export_dataframe_to_excel(dataframe = df, filepath=filepath, encoding=encoding,  
        header=header, index=index,float_format=float_format, sheet_name=sheet_name, 
        date_format=date_format)


def load_dataframe_to_hana(dataframe, schema, table, if_exists='replace'):

    schema = schema.lower()
    table = table.lower()

    types = dict(dataframe.dtypes)

    if len(dataframe) > 0:
        clob_list = [key for key in types if types[key] is dtype('O')]
        try:
            new_types = {key: String(round(max(dataframe[key].apply(
                str).apply(len)) * 1.1) + 1) for key in clob_list}
            dataframe.to_sql(table, con=engine, schema=schema,
                                if_exists=if_exists, dtype=new_types, index=False)
        except:
            try:
                new_types = {key: String(round(max(dataframe[key].apply(
                    str).apply(len)) * 1.3) + 10) for key in clob_list}
                dataframe.to_sql(table, con=engine, schema=schema,
                                if_exists=if_exists, dtype=new_types, index=False)
            except:
                new_types = {key: String(
                    round(max(dataframe[key].apply(str).apply(len)) * 2) + 30) for key in clob_list}
                dataframe.to_sql(table, con=engine, schema=schema,
                             if_exists=if_exists, dtype=new_types, index=False)


def rename_table_hana(origin_table, destination_table):

    run_query(""" RENAME TABLE {} TO {} """.format(origin_table, destination_table))


def get_datetime_from_table(schema, table, campoData, date_format='%Y-%m-%d %H:%M:%S.%f'):
    
    query = f""" SELECT MAX({campoData}) AS DATE FROM "{schema}"."{table}" """
    
    last_updated_date = str(run_select(query)['date'][0])[:10]
    last_updated_date = datetime.strptime(last_updated_date, date_format)

    return last_updated_date


def import_files_from_path_to_hana(path, schema):
    """
    Import file types: XLS, XLSX e CSV

    Its necessary to send a string with the path of the files without the 
    slash at the end of the directory

    Import only files in the current folder, not sub-folders
    """
    for filename in os.listdir('{}\\'.format(path)):
        dataframe = {}

        filepath = "{}\{}".format(path, filename)

        if filename.endswith("xlsx") or filename.endswith("xls"):
            dataframe = tools.read_excel_file(filepath=filepath)

        elif filename.endswith('csv'):
            try:
                dataframe = tools.read_csv_file(filepath, sep=';', encoding=None, engine='python')
            except:
                dataframe = tools.read_csv_file(filepath, sep=',', encoding=None, engine='python')

        # Check if data was insert into data_frame
        if len(dataframe) > 0:
            table = filename.replace('.xlsx', '').replace(
                '.xls', '').replace('.csv', '').upper()

            dataframe['DT_ATZ_LOG'] = datetime.now()

            drop_table(schema=schema, table=table)

            load_dataframe_to_hana(dataframe=dataframe,
                          schema=schema,
                          table=table)
