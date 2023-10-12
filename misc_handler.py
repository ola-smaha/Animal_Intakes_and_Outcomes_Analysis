import os
from lookups import SQLCommandsPath

def get_sql_files_list(sql_commands_path = SQLCommandsPath.SQL_FOLDER):
    sql_files = [file for file in os.listdir(sql_commands_path.value) if file.endswith('.sql')]
    sorted_sql_files = sorted(sql_files)
    return sorted_sql_files
