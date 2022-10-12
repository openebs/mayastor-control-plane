import shutil


class Fio(object):
    def __init__(
        self, name, rw, device="", runtime=15, optstr="", traddr="", subnqn=""
    ):
        self.subnqn = subnqn
        self.traddr = traddr
        self.name = name
        self.rw = rw
        self.device = device
        self.cmd = shutil.which("fio")
        self.output = {}
        self.success = {}
        self.runtime = runtime
        self.optstr = optstr

    def build(self):
        devs = [self.device] if isinstance(self.device, str) else self.device

        command = (
            "sudo fio --ioengine=linuxaio --direct=1 --bs=4k "
            "--time_based=1 {} --rw={} "
            "--group_reporting=1 --norandommap=1 --iodepth=64 "
            "--runtime={} --name={} --filename={}"
        ).format(
            self.optstr, self.rw, self.runtime, self.name, ":".join(map(str, devs))
        )

        return command

    def build_for_userspace(self):
        command = (
            "docker exec -i fio-spdk fio --name={} --filename='trtype=tcp adrfam=IPv4 traddr={} trsvcid=8420 "
            "subnqn={} ns=1' --direct=1 --rw={} --ioengine=spdk --bs=4k --iodepth=64 --numjobs=1 --thread=1 "
            "--size=50M --norandommap=1".format(
                self.name, self.traddr, self.subnqn.replace(":", "\\:"), self.rw
            )
        )
        return command
