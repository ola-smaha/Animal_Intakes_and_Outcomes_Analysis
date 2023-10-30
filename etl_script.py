from logging_handler import setup_logging
from prehook import execute_prehook
from hook import execute_hook
from posthook import execute_posthook
import schedule
import time

def etl_job():
    logger = setup_logging()
    execute_prehook(logger)
    execute_hook(logger)
    execute_posthook(logger)

# schedule.every(1).day.do(etl_job)

# while True:
#     schedule.run_pending()
#     time.sleep(12*3600)
