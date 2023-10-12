from database_handler import execute_query, create_statement_from_df, create_connection, close_connection
from logging_handler import show_error_msg  
from lookups import Errors, PreHookSteps, SQLCommandsPath, DestinationSchemaName
import os
from transformation_handler import clean_all_data
from misc_handler import get_sql_files_list

def execute_sql_folder_prehook(db_session, target_schema = DestinationSchemaName.Datawarehouse, sql_commands_path = SQLCommandsPath.SQL_FOLDER):
    sql_files = None
    try:
        sql_files = get_sql_files_list(sql_commands_path)
        for file in sql_files:
            if '_prehook' in file:
                with open(os.path.join(sql_commands_path.value,file), 'r') as f:
                    sql_query = f.read()
                    sql_query = sql_query.replace('target_schema', target_schema.value)
                    return_val = execute_query(db_session= db_session, query= sql_query)
                    if not return_val == Errors.NO_ERROR:
                        raise Exception(f"{PreHookSteps.EXECUTE_SQL_QUERIES.value} = SQL File Error on SQL FILE = " +  str(file))
    except Exception as e:
        show_error_msg(Errors.PREHOOK_SQL_ERROR.value, str(e))
    finally:
        return sql_files

def create_sql_stg_table_idx(db_session,source_name,table_name,index_val):
    try:
        query = f"CREATE INDEX IF NOT EXISTS idx_{table_name} ON {source_name}.{table_name} ({index_val});"
        execute_query(db_session,query)
    except Exception as e:
        show_error_msg(PreHookSteps.CREATE_TABLE_IDX.value, str(e))


def create_sql_staging_table(db_session, target_schema):
    source_dfs = clean_all_data(limit=1) 
    if len(source_dfs) == 0:
        raise Exception("No DataFrame returned by clean_all_data()")
    create_stmt = None
    try:
        for stg_name, stg_df in source_dfs.items():
            staging_df = stg_df.head(1)
            columns = list(staging_df.columns)
            create_stmt = create_statement_from_df(staging_df, f"{target_schema.value}", "stg_"+stg_name)
            execute_return_val = execute_query(db_session=db_session, query=create_stmt)
            if execute_return_val != Errors.NO_ERROR:
                raise Exception(f"{Errors.EXECUTE_QUERY_ERROR}: error occurred while creating table stg_{stg_name}.")
            create_sql_stg_table_idx(db_session, f"{target_schema.value}", "stg_"+stg_name, columns[0])
    except Exception as e:
        show_error_msg(PreHookSteps.CREATE_SQL_STAGING.value, str(e))
    finally:
        return create_stmt


def execute_prehook(sql_commands_path = SQLCommandsPath.SQL_FOLDER):
    step = None
    try:
        step = 1
        db_session = create_connection()
        step = 2
        execute_sql_folder_prehook(db_session,DestinationSchemaName.Datawarehouse,sql_commands_path)
        step = 3
        create_sql_staging_table(db_session,DestinationSchemaName.Datawarehouse)
        step = 4
        close_connection(db_session)
    except Exception as e:
        error_prefix = f'{Errors.PREHOOK_SQL_ERROR.value} on step {step}'
        show_error_msg(error_prefix,str(e))