#!/usr/bin/env python3
import os
from sqlalchemy.engine.base import Engine
from sqlalchemy import create_engine
from configparser import ConfigParser
from operator import itemgetter
import pandas as pd


CONFIG_FILE = "config.ini"
# PASSWORD = os.getenv('PASSWORD')
EXTRACTED_LOC="extracted_files"


def read_config() -> ConfigParser:
    config = ConfigParser()
    config.read(filenames=CONFIG_FILE)
    return config

def get_section_config(config: ConfigParser, section: str) -> dict:
    return dict(config.items(section=section))


def get_password() -> str:
    password = os.getenv('PASSWORD')
    if password is None:
        return 1234
    return password


def get_connection(config: dict) -> Engine:
    host, port, user, db, dbtype, library, driver = \
        itemgetter('host', 'port', 'user', 'db', 'dbtype', 'library', 'driver')(config)
    connection_uri = f"{dbtype}+{library}://{user}:{get_password()}@{host}:{port}/{db}{driver}"
    # print(connection_uri)
    return create_engine(connection_uri)


def extract_table(con: Engine, schema: str, table: str) -> pd.DataFrame:
    sql = f"select * from {schema}.{table}"
    return pd.read_sql(con=con, sql=sql)

def load_table(con:Engine,table:str,file_path:str):
    df=pd.read_csv(filepath_or_buffer=file_path,sep=',')
    df.to_sql(con=con, name=table, if_exists="replace")


def write_csv(df:pd.DataFrame,file_path=str):
    df.to_csv(path_or_buf=file_path, sep=',', index=False)

def create_dir(section:str) ->str :
    file_dir=f"{EXTRACTED_LOC}/{section}"
    os.makedirs(file_dir, exist_ok=True)
    return file_dir


def extract_all_data(source:str, target:str):
    config = read_config()
    source_config = get_section_config(config=config, section=source)

    source_con = get_connection(source_config)
    source_schema = source_config['schema']
    source_tables = source_config['tables'].split(',')

    target_config = get_section_config(config=config, section=target)
    target_con = get_connection(target_config)
    target_schema = source_config['schema']

    for table in source_tables:
        df = extract_table(con=source_con, schema=source_schema, table=table)
        extracted_stuffs = create_dir(section=source)
        file_path = f"{extracted_stuffs}/{table}.csv"
        write_csv(df=df, file_path=file_path)
        load_table(con=target_con, table=table, file_path=file_path)

if __name__ == "__main__":
    # pass
    try:
        extract_all_data(source='local_sqlserver1_connection', target='local_pgcon1')
        print("Successfully Data Loaded")
    except Exception as e:
        print("An Error Occured: ", e)