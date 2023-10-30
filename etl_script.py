from logging_handler import setup_logging
from prehook import execute_prehook
from hook import execute_hook
from posthook import execute_posthook
import schedule
import time

def etl_job():
    print("ETL job is running...\n")
    logger = setup_logging()
    execute_prehook(logger)
    execute_hook(logger)
    execute_posthook(logger)

# Scheduling the job to be run every day
schedule.every(1).day.do(etl_job)

while True:
    schedule.run_pending()
    time.sleep(6*3600)
