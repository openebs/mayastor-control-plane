from urllib.parse import urlparse, parse_qs, ParseResult
from retrying import retry
from common.command import run_cmd_async_at

import subprocess
import time
import json


async def nvme_remote_connect(remote, uri):
    """Connect to the remote nvmf target on this host."""
    u = urlparse(uri)
    port = u.port
    host = u.hostname
    nqn = u.path[1:]
    hostnqn = nvme_hostnqn_arg(u)

    command = f"sudo nvme connect -t tcp -s {port} -a {host} -n {nqn} {hostnqn}"

    await run_cmd_async_at(remote, command)
    return wait_nvme_find_device(uri)


async def nvme_remote_disconnect(remote, uri):
    """Disconnect the given URI on this host."""
    u = urlparse(uri)
    nqn = u.path[1:]

    command = "sudo nvme disconnect -n {0}".format(nqn)
    await run_cmd_async_at(remote, command)


async def nvme_remote_discover(remote, uri):
    """Discover target."""
    u = urlparse(uri)
    port = u.port
    host = u.hostname
    hostnqn = nvme_hostnqn_arg(u)

    command = f"sudo nvme discover -t tcp -s {port} -a {host} {hostnqn}"
    output = await run_cmd_async_at(remote, command).stdout
    if not u.path[1:] in str(output.stdout):
        raise ValueError("uri {} is not discovered".format(u.path[1:]))


def nvme_hostnqn_arg(uri: ParseResult):
    uri_query = parse_qs(uri.query)
    if uri_query.keys().__contains__("hostnqn"):
        return "-q {}".format(uri_query["hostnqn"][0])
    else:
        return ""


def nvme_connect(uri):
    u = urlparse(uri)
    port = u.port
    host = u.hostname
    nqn = u.path[1:]
    hostnqn = nvme_hostnqn_arg(u)

    command = f"sudo nvme connect -t tcp -s {port} -a {host} -n {nqn} {hostnqn}"
    print(command)
    subprocess.run(command, check=True, shell=True, capture_output=False)

    return wait_nvme_find_device(uri)


def nvme_id_ctrl(device):
    """Identify controller."""
    command = "sudo nvme id-ctrl {0} -o json".format(device)
    id_ctrl = json.loads(
        subprocess.run(
            command, shell=True, check=True, text=True, capture_output=True
        ).stdout
    )

    return id_ctrl


def nvme_resv_report(device):
    """Reservation report."""
    command = "sudo nvme resv-report {0} -c 1 -o json".format(device)
    resv_report = json.loads(
        subprocess.run(
            command, shell=True, check=True, text=True, capture_output=True
        ).stdout
    )

    return resv_report


def nvme_discover(uri):
    """Discover target."""
    u = urlparse(uri)
    port = u.port
    host = u.hostname
    hostnqn = nvme_hostnqn_arg(u)

    command = f"sudo nvme discover -t tcp -s {port} -a {host} {hostnqn}"
    output = subprocess.run(
        command, check=True, shell=True, capture_output=True, encoding="utf-8"
    )
    if not u.path[1:] in str(output.stdout):
        raise ValueError("uri {} is not discovered".format(u.path[1:]))


def nvme_find_device(uri):
    u = urlparse(uri)
    nqn = u.path[1:]

    command = "sudo nvme list -v -o json"
    discover = json.loads(
        subprocess.run(
            command, shell=True, check=True, text=True, capture_output=True
        ).stdout
    )

    dev = list(
        filter(
            lambda d: list(
                filter(lambda dd: nqn in dd.get("SubsystemNQN"), d.get("Subsystems"))
            ),
            discover.get("Devices"),
        )
    )
    # we should only have one connection
    assert len(dev) == 1
    assert len(dev[0].get("Subsystems")) == 1
    subsystem = dev[0].get("Subsystems")[0]

    return "/dev/{}".format(subsystem["Namespaces"][0].get("NameSpace"))


def nvme_disconnect(uri):
    """Disconnect the given URI on this host."""
    u = urlparse(uri)
    nqn = u.path[1:]

    command = "sudo nvme disconnect -n {0}".format(nqn)
    subprocess.run(command, check=True, shell=True, capture_output=True)


def nvme_disconnect_all():
    """Disconnect from all connected nvme subsystems"""
    command = "sudo nvme disconnect-all"
    subprocess.run(command, check=True, shell=True, capture_output=True)


def nvme_disconnect_controller(name):
    """Disconnect the given NVMe controller on this host."""
    command = "sudo nvme disconnect -d {0}".format(name)
    subprocess.run(command, check=True, shell=True, capture_output=True)


def nvme_list_subsystems(device):
    """Retrieve information for NVMe subsystems"""
    command = "sudo nvme list-subsys {} -o json".format(device)
    subsystems = json.loads(
        subprocess.run(
            command, check=True, shell=True, capture_output=True, encoding="utf-8"
        ).stdout
    )
    return list(filter(lambda s: len(s["Subsystems"]) == 1, subsystems))[0]


NS_PROPS = ["nguid", "eui64"]


def identify_namespace(device):
    """Get properties of a namespace on this host"""
    command = "sudo nvme id-ns {}".format(device)
    output = subprocess.run(
        command, check=True, shell=True, capture_output=True, encoding="utf-8"
    )
    props = output.stdout.strip().split("\n")[1:]
    ns = {}
    for p in props:
        v = [v.strip() for v in p.split(":") if p.count(":") == 1]
        if len(v) == 2 and v[0] in NS_PROPS:
            ns[v[0]] = v[1]
    return ns


def nvme_set_reconnect_delay(uri, delay=10):
    u = urlparse(uri)
    nqn = u.path[1:]

    command = "sudo nvme list -v -o json"
    discover = json.loads(
        subprocess.run(
            command, shell=True, check=True, text=True, capture_output=True
        ).stdout
    )
    dev = list(
        filter(
            lambda d: list(
                filter(lambda dd: nqn in dd.get("SubsystemNQN"), d.get("Subsystems"))
            ),
            discover.get("Devices"),
        )
    )
    # we should only have one connection
    assert len(dev) == 1
    assert len(dev[0].get("Subsystems")) == 1
    subsystem = dev[0].get("Subsystems")[0]

    controller = subsystem["Controllers"][0].get("Controller")

    command = f"echo {delay} | sudo tee -a /sys/class/nvme/{controller}/reconnect_delay"
    subprocess.run(command, check=True, shell=True, capture_output=True)


@retry(wait_fixed=100, stop_max_attempt_number=20)
def wait_nvme_find_device(uri):
    return nvme_find_device(uri)
