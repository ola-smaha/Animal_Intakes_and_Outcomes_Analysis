from database_handler import execute_query, create_statement_from_df, create_connection, close_connection
from logging_handler import log_error_msg, setup_logging
from lookups import Errors, PreHookSteps, SQLCommandsPath, DataWareHouseSchema
import os
from data_extraction_handler import readData
from transformation_handler import clean_all_data
from misc_handler import get_sql_files_list


def execute_sql_folder_prehook(db_session, target_schema = DataWareHouseSchema.SCHEMA_NAME, sql_commands_path = SQLCommandsPath.SQL_FOLDER):
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
        log_error_msg(Errors.PREHOOK_SQL_ERROR.value, str(e))
    finally:
        return sql_files

def create_sql_stg_table_idx(db_session,source_name,table_name,index_val):
    try:
        query = f"CREATE INDEX IF NOT EXISTS idx_{table_name} ON {source_name}.{table_name} ({index_val});"
        execute_query(db_session,query)
    except Exception as e:
        log_error_msg(PreHookSteps.CREATE_TABLE_IDX.value, str(e))


def create_sql_staging_table(db_session, target_schema):
    create_stmt = None
    try:
        dfs = readData(etl_date=None,limit=1)
        source_dfs = clean_all_data(dfs)
        if len(source_dfs) == 0:
            raise Exception("No DataFrame returned by clean_all_data")
        for stg_name, stg_df in source_dfs.items():
            staging_df = stg_df.head(1)
            columns = list(staging_df.columns)
            create_stmt = create_statement_from_df(staging_df, f"{target_schema.value}", "stg_"+stg_name)
            execute_return_val = execute_query(db_session=db_session, query=create_stmt)
            if execute_return_val != Errors.NO_ERROR:
                raise Exception(f"{Errors.EXECUTE_QUERY_ERROR}: Error occurred while creating table stg_{stg_name}")
            create_sql_stg_table_idx(db_session, f"{target_schema.value}", "stg_"+stg_name, columns[0])
    except Exception as e:
        log_error_msg(PreHookSteps.CREATE_SQL_STAGING.value, str(e))
    finally:
        return create_stmt

# def execute_prehook(sql_commands_path = SQLCommandsPath.SQL_FOLDER):
#     step = None
#     try:
#         step = 1
#         db_session = create_connection()
#         step = 2
#         execute_sql_folder_prehook(db_session,DataWareHouseSchema.SCHEMA_NAME,sql_commands_path)
#         step = 3
#         create_sql_staging_table(db_session,DataWareHouseSchema.SCHEMA_NAME)
#         step = 4
#         close_connection(db_session)
#     except Exception as e:
#         error_prefix = f'{Errors.PREHOOK_SQL_ERROR.value} on step {step}'
#         log_error_msg(error_prefix,str(e))

def execute_prehook(logger, sql_commands_path=SQLCommandsPath.SQL_FOLDER):
    step = None
    logger.info("Executing Prehook:")
    try:
        step = 1
        logger.info("Step 1: Creating a database connection")
        db_session = create_connection()

        step = 2
        logger.info("Step 2: Executing SQL folder prehook")
        execute_sql_folder_prehook(db_session, DataWareHouseSchema.SCHEMA_NAME, sql_commands_path)

        step = 3
        logger.info("Step 3: Creating SQL staging table")
        create_sql_staging_table(db_session, DataWareHouseSchema.SCHEMA_NAME)

        step = 4
        logger.info("Step 4: Closing the database connection\n")
        close_connection(db_session)
    except Exception as e:
        error_prefix = f'{Errors.PREHOOK_SQL_ERROR.value} on step {step}'
        logger.error(f'{error_prefix} - {str(e)}')