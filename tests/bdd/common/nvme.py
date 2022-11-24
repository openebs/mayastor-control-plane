from urllib.parse import urlparse, parse_qs, ParseResult
import subprocess
import time
import json
from common.command import run_cmd_async_at


async def nvme_remote_connect(remote, uri):
    """Connect to the remote nvmf target on this host."""
    u = urlparse(uri)
    port = u.port
    host = u.hostname
    nqn = u.path[1:]
    hostnqn = nvme_hostnqn_arg(u)

    command = f"sudo nvme connect -t tcp -s {port} -a {host} -n {nqn} {hostnqn}"

    await run_cmd_async_at(remote, command)
    time.sleep(1)
    command = "sudo nvme list -v -o json"

    discover = await run_cmd_async_at(remote, command)
    discover = json.loads(discover.stdout)

    dev = list(filter(lambda d: nqn in d.get("SubsystemNQN"), discover.get("Devices")))

    # we should only have one connection
    assert len(dev) == 1
    dev_path = dev[0].get("Controllers")[0].get("Namespaces")[0].get("NameSpace")

    return f"/dev/{dev_path}"


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
    subprocess.run(command, check=True, shell=True, capture_output=False)
    time.sleep(1)
    command = "sudo nvme list -v -o json"
    discover = json.loads(
        subprocess.run(
            command, shell=True, check=True, text=True, capture_output=True
        ).stdout
    )

    dev = list(filter(lambda d: nqn in d.get("SubsystemNQN"), discover.get("Devices")))

    # we should only have one connection
    assert len(dev) == 1
    try:
        device = "/dev/{}".format(dev[0]["Namespaces"][0].get("NameSpace"))
    except KeyError:
        device = "/dev/{}".format(
            dev[0]["Controllers"][0]["Namespaces"][0].get("NameSpace")
        )
    return device


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

    dev = list(filter(lambda d: nqn in d.get("SubsystemNQN"), discover.get("Devices")))

    # we should only have one connection
    assert len(dev) == 1

    # The device list seems to differ locally and CI, possibly due to kernel differences
    # See the example snippet for some details
    # 5.10.52 #1-NixOS SMP Tue Jul 20 14:05:59 UTC 2021 x86_64 GNU/Linux
    #   Controllers:
    #   - Controller: nvme2
    #     Namespaces:
    #     - NameSpace: nvme2c2n1
    #       NSID: 1
    #   Namespaces:
    #   - NameSpace: nvme2n1
    #     NSID: 1
    #  VS
    # 5.18.10 #1-NixOS SMP PREEMPT_DYNAMIC Thu Jul 7 15:55:01 UTC 2022 x86_64 GNU/Linux
    #   Controllers:
    #   - Controller: nvme2
    #     Namespaces:
    #     - NameSpace: nvme2n1
    #       NSID: 1
    if "Namespaces" in dev[0]:
        return "/dev/{}".format(dev[0]["Namespaces"][0].get("NameSpace"))
    return "/dev/{}".format(
        dev[0]["Controllers"][0].get("Namespaces")[0].get("NameSpace")
    )


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
    return json.loads(
        subprocess.run(
            command, check=True, shell=True, capture_output=True, encoding="utf-8"
        ).stdout
    )


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
