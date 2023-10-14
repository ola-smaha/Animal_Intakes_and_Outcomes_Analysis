from database_handler import *
from logging_handler import log_error_msg
from lookups import Errors, HookSteps, DataWareHouseSchema, SQLCommandsPath, ETL_Checkpoint
from misc_handler import get_sql_files_list
import os
import transformation_handler

def execute_sql_folder_hook(db_session, target_schema = DataWareHouseSchema.SCHEMA_NAME, sql_commands_path = SQLCommandsPath.SQL_FOLDER):
    sql_files = None
    try:
        sql_files = get_sql_files_list(sql_commands_path)
        for file in sql_files:
            if '_hook' in file:
                with open(os.path.join(sql_commands_path.value,file), 'r') as f:
                    sql_query = f.read()
                    sql_query = sql_query.replace('target_schema', target_schema.value)
                    return_val = execute_query(db_session= db_session, query= sql_query)
                    if not return_val == Errors.NO_ERROR:
                        raise Exception(f"{HookSteps.EXECUTE_SQL_QUERIES.value} = Error on SQL FILE = " +  str(file))
    except Exception as e:
        log_error_msg(Errors.HOOK_SQL_ERROR.value, str(e))
    finally:
        return sql_files
    
def create_etl_checkpoint(target_schema, db_session):
    query = None
    try:
        query = f"""CREATE TABLE IF NOT EXISTS {target_schema.value}.{ETL_Checkpoint.TABLE.value}
        (
            {ETL_Checkpoint.COLUMN.value} TIMESTAMP
        );
        """
        execute_query(db_session, query)
    except Exception as e:
        log_error_msg(HookSteps.CREATE_ETL_CHECKPOINT.value,str(e))
    finally:
        return query
    
def insert_or_update_etl_checkpoint(db_session,
                                    etl_time_exists = False,
                                    target_schema = DataWareHouseSchema.SCHEMA_NAME,
                                    table_name = ETL_Checkpoint.TABLE,
                                    column_name = ETL_Checkpoint.COLUMN):
    try: 
        if etl_time_exists:
            update_query = f"""
                UPDATE {target_schema.value}.{table_name.value}
                SET {column_name.value} = '{datetime.datetime.now()}'
            """
            execute_query(db_session=db_session, query=update_query)
        else:
            insert_query = f"""
                INSERT INTO {target_schema.value}.{table_name.value}
                VALUES('{ETL_Checkpoint.ETL_DEFAULT_DATE.value}')
            """
            execute_query(db_session=db_session, query=insert_query)
    except Exception as e:
        log_error_msg(HookSteps.INSERT_UPDATE_ETL_CHECKPOINT.value,str(e))

def return_etl_last_updated_date(db_session,
                                target_schema = DataWareHouseSchema.SCHEMA_NAME,
                                etl_date = ETL_Checkpoint.ETL_DEFAULT_DATE,
                                table_name = ETL_Checkpoint.TABLE,
                                column_name = ETL_Checkpoint.COLUMN):
    etl_time_exists = False
    return_date = None
    try:
        query = f"SELECT {column_name.value} FROM {target_schema.value}.{table_name.value} ORDER BY {column_name.value} DESC LIMIT 1"
        etl_df = read_data_as_dataframe(file_type = InputTypes.SQL, file_path = query, db_session= db_session)
        if len(etl_df) == 0:
            return_date = etl_date.value
        else:
            return_date = etl_df[column_name.value].iloc[0]
            etl_time_exists = True
    except Exception as e:
        log_error_msg(HookSteps.RETURN_LAST_ETL_RUN.value, str(e))
    finally:    
        return return_date, etl_time_exists
    

