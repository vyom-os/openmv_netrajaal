from collections import deque

from clock_utils import format_epochms_str, get_epoch_ms, timestamp_str
from config import save_log_entry

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


def log_internal(level_message):
    try:
        timestamp = format_epochms_str(get_epoch_ms())
        line = f"{timestamp} {level_message}"
    except Exception:
        line = level_message
    print(line)
    saved_logs.append(line)
    try:
        import db_store
        db_store.append_log_line(line)
    except Exception as e:
        print(e)


def info(m):
    log_internal(f"[info] : {m}")


def debug(m):
    pass
    # log_internal(f"[debug] : {m}")


def warning(m):
    log_internal(f"[WARNING] : {m}")


def error(m):
    log_internal(f"[ERROR] : {m}")


def fatal(m):
    log_internal(f"[FATAL] : {m}")
    if SAVE_FATAL_LOGS:
        try:
            save_log_entry(f"{timestamp_str()} [FATAL]: {m}")
        except Exception as e:
            pass
