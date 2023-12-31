from database_handler import execute_query, create_connection, close_connection
from lookups import StagingTablesNames, DataWareHouseSchema, Errors
from logging_handler import log_error_msg


def truncate_staging_table(db_session, source_name=DataWareHouseSchema.SCHEMA_NAME):
    try:
        for table in StagingTablesNames:
            dst_table = f"{source_name.value}.{table.value}"
            truncate_query = f"""
                TRUNCATE TABLE {dst_table}
            """
            return_value = execute_query(db_session, truncate_query)
            if not return_value == Errors.NO_ERROR:
                raise Exception(f"Failed to truncate '{dst_table}'.")
    except Exception as e:
        log_error_msg(Errors.POSTHOOK_TRUNCATE_ERROR.value, str(e))


def execute_posthook(logger):
    step = None
    logger.info("Executing posthook:")
    try:
        step = 1
        logger.info("Step 1: Creating a database connection")
        db_session = create_connection()

        step = 2
        logger.info("Step 2: Truncating staging table")
        truncate_staging_table(db_session, source_name=DataWareHouseSchema.SCHEMA_NAME)

        step = 3
        logger.info("Step 3: Closing the database connection\n")
        close_connection(db_session)

    except Exception as e:
        error_prefix = f'{Errors.EXECUTE_POSTHOOK_ERROR.value} - Error on step {step}'
        logger.error(f'{error_prefix} - {str(e)}')



