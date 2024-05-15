import os
import re
import time


prod_domain_name = "openebs.io"
prod_rev_domain_name = "io.openebs"
prod_name = "mayastor"
nvme_nqn_prefix = f"nqn.2019-05.{prod_rev_domain_name}"


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
