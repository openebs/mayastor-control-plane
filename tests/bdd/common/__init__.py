import os
import re
import time


# Converts humantime to float seconds
# Example: 100ms -> 0.1
def human_time_to_float(human_time) -> float:
    conv = {"s": 1, "ms": 100, "us": 1000}
    m = re.match("(?P<num>\\d+)(?P<unit>.*)", human_time)
    matches = m.groupdict()
    num = int(matches["num"])
    unit = matches["unit"]
    assert unit in conv
    return num / float(conv[unit])


def human_sleep(human_time):
    time.sleep(human_time_to_float(human_time))


def env_cleanup():
    clean = os.getenv("CLEAN")
    if clean is not None and clean.lower() in ("no", "false", "f", "0"):
        return False
    return True
