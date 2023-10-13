import psycopg2
from lookups import Errors, InputTypes
from logging_handler import log_error_msg
import pandas as pd
from datetime import time, datetime

config_dict = {
    "db_name" : "animal_intakes_outcomes",
    "user" : "postgres",
    "password" : "admin",
    "host" : "localhost",
    "port" : 5432
    }

def create_connection():
    db_session = None
    print(f'Connecting to database "{config_dict.get("db_name")}"...')
    try:
        db_session = psycopg2.connect(
            database = config_dict.get('db_name'), 
            user = config_dict.get('user'), 
            password = config_dict.get('password'), 
            host = config_dict.get('host'), 
            port = config_dict.get('port')
        )
        print('Connection was successful.')
    except Exception as e:
        log_error_msg(Errors.ERROR_CONNECTING_TO_DB.value, str(e))
    finally:
        return db_session

def return_query(db_session,query):
    result = None
    try:
        cursor = db_session.cursor()
        cursor.execute(query)
        result = cursor.fetchall()
        db_session.commit()
    except Exception as e:
        log_error_msg(Errors.DB_RETURN_QUERY_ERROR.value, str(e))
    finally:
        return result

def read_data_as_dataframe(file_type, file_path, db_session = None):
    return_dataframe = None
    try:
        if file_type == InputTypes.CSV:
            return_dataframe = pd.read_csv(file_path)
        elif file_type == InputTypes.SQL:
            return_dataframe = pd.read_sql_query(con= db_session, sql= file_path)
        else:
            raise Exception("File type does not exist.")
    except Exception as e:
        error_suffix = str(e)
        if file_type == InputTypes.CSV:
            error_prefix = Errors.RETURN_DF_CSV_ERROR.value
        elif file_type == InputTypes.SQL:
            error_prefix = Errors.RETURN_DF_SQL_ERROR.value
        else:
            error_prefix = Errors.UNDEFINED_ERROR.value
        log_error_msg(error_prefix, error_suffix)
    finally:
        return return_dataframe
    
def execute_query(db_session, query):
    return_val = Errors.NO_ERROR
    try:
        cursor = db_session.cursor()
        cursor.execute(query)
        db_session.commit()
    except Exception as e:
        return_val = Errors.EXECUTE_QUERY_ERROR
        log_error_msg(Errors.EXECUTE_QUERY_ERROR.value, str(e))
    finally:
        if cursor:
            cursor.close()
        return return_val

def create_statement_from_df(df, schema_name, table_name):
    create_table_stmt = None 
    try:
        data_type_mapping = {
            'int64' : 'INTEGER', 
            'float64' : 'FLOAT', 
            'bool' : 'BOOLEAN', 
            'object' : 'TEXT', 
            'datetime64[ns]' : 'TIMESTAMP'
        }
        cols = [f"{column} {data_type_mapping.get(str(dtype), 'TEXT')}" for column, dtype in df.dtypes.items()]
        create_table_stmt = f"CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} (\n"
        create_table_stmt += ", \n".join(cols)
        create_table_stmt += "\n);"
    except Exception as e:
        log_error_msg(Errors.ERROR_CREATE_STMNT.value, str(e))
    finally:
        return create_table_stmt

    
def insert_into_sql_statement_from_df(df, schema_name, table_name):
    insert_statement = None
    try:
        column_names = ', '.join(df.columns)
        values_list = []
        for _, row in df.iterrows():
            values_strs = []
            for val in row.values:
                if isinstance(val, str):
                    val_escaped = val.replace("'", "''")
                    values_strs.append(f"'{val_escaped}'")
                elif isinstance(val, datetime):
                    values_strs.append(f"'{val}'")
                elif pd.isna(val):
                    values_strs.append("NULL")
                else:
                    values_strs.append(str(val))
            values = ', '.join(values_strs)
            values_list.append(f"({values})")
        values_str = ',\n'.join(values_list)
        insert_statement = f"INSERT INTO {schema_name}.{table_name} ({column_names}) VALUES\n{values_str};"
    except Exception as e:
        log_error_msg(Errors.ERROR_INSERT_STMNT, str(e))
    finally:
        return insert_statement

def refresh_connection(db_session):
    db_session.close()
    time.sleep(5)
    db_session = create_connection()
    return db_session

def close_connection(db_session):
    return_val = None
    print("Closing connection to database...")
    try:
        return_val = db_session.close()
        print("Connection is closed.")
    except Exception as e:
        log_error_msg(Errors.ERROR_CLOSING_CONN.value,str(e))
    finally:
        return return_val