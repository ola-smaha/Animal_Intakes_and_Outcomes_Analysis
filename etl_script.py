from logging_handler import setup_logging
from prehook import execute_prehook
from hook import execute_hook
from posthook import execute_posthook

logger = setup_logging()
execute_prehook(logger)
execute_hook(logger)
execute_posthook(logger)