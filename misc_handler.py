import os
from lookups import SQLCommandsPath, Errors
from logging_handler import log_error_msg

def get_sql_files_list(sql_commands_path = SQLCommandsPath.SQL_FOLDER):
    sql_files = []
    try:
        sql_files = [file for file in os.listdir(sql_commands_path.value) if file.endswith('.sql')]
        sorted_sql_files = sorted(sql_files)
    except Exception as e:
        log_error_msg(Errors.GET_SQL_FILES_LIST.value, str(e))
    finally:
        return sorted_sql_files
