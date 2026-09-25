import os
from collections import deque
from config import save_log_entry
from clock_utils import format_epochms_str, get_epoch_ms
from utils import print_exception

SAVE_FATAL_LOGS = True

LOG_PATH = "/vyomos/log.txt"
LOG_TMP_PATH = "/vyomos/log.tmp"
LOG_MAX_BYTES = 5 * 1024 * 1024
LOG_TRIM_BYTES = 1 * 1024 * 1024
_TRIM_CHUNK = 4096

log_q_len = 100
saved_logs = deque([], log_q_len)

def get_saved_logs():
    return saved_logs
def return_saved_logs_and_clear():
    global saved_logs
    to_ret = saved_logs
    saved_logs = deque([], log_q_len)
    return to_ret

def _file_size(path):
    try:
        return os.stat(path)[6]
    except OSError:
        print_exception()
        return 0

def _drop_oldest_logs(path, drop_bytes):
    """Drop the oldest drop_bytes from path, keeping the newer tail."""
    size = _file_size(path)
    if size <= drop_bytes:
        try:
            os.remove(path)
        except OSError:
            print_exception()
            pass
        return
    try:
        os.remove(LOG_TMP_PATH)
    except OSError:
        print_exception()
        pass
    buf = bytearray(_TRIM_CHUNK)
    mv = memoryview(buf)
    src = open(path, "rb")
    try:
        src.seek(drop_bytes)
        extra = src.read(256)
        nl = extra.find(b"\n")
        pending = extra[nl + 1:] if nl >= 0 else extra
        dst = open(LOG_TMP_PATH, "wb")
        try:
            if pending:
                dst.write(pending)
            while True:
                n = src.readinto(buf)
                if not n:
                    break
                dst.write(mv[:n])
        finally:
            dst.close()
    finally:
        src.close()
    old_path = path + ".old"
    os.rename(path, old_path)
    try:
        os.rename(LOG_TMP_PATH, path)
    except Exception:
        print_exception()
        os.rename(old_path, path)
        raise
    try:
        os.remove(old_path)
    except OSError:
        print_exception()
        pass
    try:
        os.sync()
    except Exception:
        print_exception()
        pass

def _append_vyomos_log(m):
    with open(LOG_PATH, "a") as f:
        f.write(m + "\n")
    # Grow through 4MB up to 5MB, then drop the oldest 1MB and repeat.
    while _file_size(LOG_PATH) >= LOG_MAX_BYTES:
        before = _file_size(LOG_PATH)
        _drop_oldest_logs(LOG_PATH, LOG_TRIM_BYTES)
        if _file_size(LOG_PATH) >= before:
            break

def _log_timestamp():
    try:
        return format_epochms_str(get_epoch_ms())
    except Exception:
        print_exception()
        return None

def log_internal(m):
    ts = _log_timestamp()
    line = f"{ts} {m}" if ts else m
    print(line)
    saved_logs.append(line)
    try:
        _append_vyomos_log(line)
    except Exception:
        print_exception()
        pass

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
            epoch_ms = get_epoch_ms()
            save_log_entry(f'{epoch_ms} [FATAL]: {m}')
        except Exception as e:
            print_exception()
            pass
