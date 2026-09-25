from collections import deque
from config import save_log_entry
from clock_utils import timestamp_str

SAVE_FATAL_LOGS = True

log_q_len = 100
saved_logs = deque([], log_q_len)

def get_saved_logs():
    return saved_logs
def return_saved_logs_and_clear():
    global saved_logs
    to_ret = saved_logs
    saved_logs = deque([], log_q_len)
    return to_ret

def log_internal(m):
    print(m)
    saved_logs.append(m)

def info(m):
    log_internal(f"{timestamp_str()} [info] : {m}")
def debug(m):
    pass
    # log_internal(f"{timestamp_str()} [debug] : {m}")
def warning(m):
    log_internal(f"{timestamp_str()} [WARNING] : {m}")
def error(m):
    log_internal(f"{timestamp_str()} [ERROR] : {m}")
def fatal(m):
    log_internal(f"{timestamp_str()} [FATAL] : {m}")
    if SAVE_FATAL_LOGS:
        try:
            save_log_entry(f'{timestamp_str()} [FATAL]: {m}')
        except Exception as e:
            pass
