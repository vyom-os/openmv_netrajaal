import utime
import machine

rtc = machine.RTC()


def get_epoch_ms():  # unix epoch milliseconds, eg. 1381791310000
    return utime.time_ns() // 1_000_000


def get_epoch_sec():  # unix epoch seconds, eg. 1736931600
    return get_epoch_ms() // 1000


def get_uptime_ms():  # milliseconds since device boot
    return utime.ticks_ms()


def get_uptime_sec():  # seconds since device boot
    return get_uptime_ms() // 1000


def format_epochms_str(epoch_ms):
    if epoch_ms is None:
        return None
    epoch_ms = int(epoch_ms)
    y, mo, d, h, mi, s, _, _ = utime.gmtime(epoch_ms // 1000)
    ms = epoch_ms % 1000
    return "{:04d}-{:02d}-{:02d} {:02d}:{:02d}:{:02d},{:03d}".format(
        y, mo, d, h, mi, s, ms
    )


def timestamp_str():
    y, mo, d, _, h, m, s, _ = rtc.datetime()
    # Get milliseconds from ticks_ms (approximate, but close enough)
    ms = utime.ticks_ms() % 1000
    return f"{y:04d}-{mo:02d}-{d:02d} {h:02d}:{m:02d}:{s:02d},{ms:03d}"


def timestamp_str_2():
    return format_epochms_str(get_epoch_ms())


def set_device_epoch_ms(epoch_ms):
    """Set the RTC from unix epoch milliseconds. Prints the previous and new time."""
    prev_ms = get_epoch_ms()
    try:
        epoch_ms = int(epoch_ms)
        # RT1062 RTC is second-precision; subseconds are always 0.
        t = utime.gmtime(epoch_ms // 1000)
        # gmtime: (Y, M, D, h, m, s, weekday, yearday)
        # RTC:    (Y, M, D, weekday, h, m, s, subseconds)       weekday 0=Mon..6=Sun
        rtc.datetime((t[0], t[1], t[2], t[6], t[3], t[4], t[5], 0))
        print(
            "Device time {} -> {}".format(
                format_epochms_str(prev_ms),
                format_epochms_str(epoch_ms),
            )
        )
        return True
    except Exception as e:
        print("Error in set_device_epoch_ms: {}".format(e))
        return False


def set_device_epoch_sec(epoch_sec):
    """Set the RTC from unix epoch seconds."""
    return set_device_epoch_ms(int(epoch_sec) * 1000)


# ========== Test cases ==========
def main_1():
    # 2024-01-01 00:00:00 UTC, then 2026-09-22 12:00:00 UTC
    target_2024_sec = 1704067200
    target_2024_ms = target_2024_sec * 1000
    target_2026_sec = 1790078400
    target_2026_ms = target_2026_sec * 1000

    def _check(name, asked_ms, set_ok, read_ms):
        read_sec = int(read_ms) // 1000
        asked_sec = int(asked_ms) // 1000
        matched = set_ok and read_sec == asked_sec
        print(name)
        print("  asked:  {}  ({})".format(format_epochms_str(asked_ms), asked_ms))
        print("  read:   {}  ({})".format(format_epochms_str(read_ms), read_ms))
        print("  result: {}".format("PASS" if matched else "FAIL"))
        print("")
        return matched

    before_ms = get_epoch_ms()
    print("")
    print("======== clock test ========")
    print("before: {}  ({})".format(format_epochms_str(before_ms), before_ms))
    print("")

    ok_2024 = set_device_epoch_sec(target_2024_sec)
    pass_2024 = _check("1) set 2024", target_2024_ms, ok_2024, get_epoch_ms())

    ok_2026 = set_device_epoch_ms(target_2026_ms)
    pass_2026 = _check("2) set 2026", target_2026_ms, ok_2026, get_epoch_ms())

    print("======== {} ========".format("TEST PASSED" if pass_2024 and pass_2026 else "TEST FAILED"))


def main_2():
    """Print 2500 logs from each timestamp function, 0.5 ms apart, and report the elapsed time."""
    count = 2500
    gap_us = 500
    target_2026_sec = 1790078400
    target_2026_ms = target_2026_sec * 1000
    ok_2026 = set_device_epoch_ms(target_2026_ms)

    print("")
    print("======== timestamp_str x{} ========".format(count))
    start = utime.ticks_ms()
    for i in range(count):
        print("timestamp_str: {}".format(timestamp_str()))
        utime.sleep_us(gap_us)
    elapsed_str_ms = utime.ticks_diff(utime.ticks_ms(), start)
    print("timestamp_str: {} logs in {} ms".format(count, elapsed_str_ms))
    print("")

    print("======== timestamp_str_2 x{} ========".format(count))
    start = utime.ticks_ms()
    for i in range(count):
        print("timestamp_str_2:  {}".format(timestamp_str_2()))
        utime.sleep_us(gap_us)
    elapsed_str2_ms = utime.ticks_diff(utime.ticks_ms(), start)
    print("timestamp_str_2: {} logs in {} ms".format(count, elapsed_str2_ms))
    print("")

    print("======== done ========")
    print("timestamp_str: {} ms".format(elapsed_str_ms))
    print("timestamp_str_2: {} ms".format(elapsed_str2_ms))


if __name__ == "__main__":
    test = 2
    if test == 1:
        main_1()
    elif test == 2:
        main_2()
