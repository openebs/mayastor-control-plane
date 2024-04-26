import shutil
from typing import Optional
from urllib.parse import parse_qs, ParseResult

import os
import subprocess

fio_bin = shutil.which("fio")


class Fio(object):
    def __init__(
        self,
        name,
        rw,
        device=None,
        runtime=None,
        uri: Optional[ParseResult] = None,
        bs="4k",
        io_depth=64,
        direct=True,
        size=None,
        norandommap=True,
        offset="0",
        extra_args="",
    ):
        self.name = name
        self.rw = rw
        self.fio_exec = shutil.which("fio")
        self.output = {}
        self.success = {}
        self.extra_args = extra_args
        self.runtime = runtime
        self.size = size
        self.bs = bs
        self.direct = direct
        self.io_depth = io_depth
        self.offset = offset
        self.norandommap = norandommap

        if device is None == uri is None:
            raise "Device and Uri as exclusive!"
        if device is not None:
            self.device = device
            self.userspace = False
        if uri is not None:
            self.uri = uri
            self.userspace = True

    def build(self):
        if self.userspace:
            return self.build_for_userspace()
        else:
            return self.build_for_kernel()

    def build_common_args(self):
        args = f"--name={self.name}"
        if self.runtime is not None:
            args = f"{args} --time_based=1 --runtime={self.runtime}"
        if self.size is not None:
            args = f"{args} --size={self.size}"
        if self.direct:
            args = f"{args} --direct=1"
        if self.norandommap:
            args = f"{args} --norandommap=1"
        args = f"{args} --offset={self.offset} --bs={self.bs} --rw={self.rw} --group_reporting=1"
        args = f"{args} --iodepth={self.io_depth} {self.extra_args}"
        return args

    def build_for_kernel(self):
        args = self.build_common_args()
        devs = [self.device] if isinstance(self.device, str) else self.device
        filename = ":".join(map(str, devs))

        command = (
            f"sudo {fio_bin} --ioengine=linuxaio --filename={filename} "
            f"--numjobs=1 --thread=1 {args} "
        )

        return command

    def build_for_userspace(self):
        args = self.build_common_args()

        uri_query = parse_qs(self.uri.query)
        traddr = self.uri.hostname
        subnqn = self.uri.path[1:].replace(":", "\\:")
        hostnqn = ""

        if uri_query.keys().__contains__("hostnqn"):
            args = "--hostnqn={} {}".format(uri_query["hostnqn"][0], args)

        command = (
            "docker exec -i fio-spdk fio --ioengine=spdk "
            f"--filename='trtype=tcp adrfam=IPv4 traddr={traddr} trsvcid=8420 subnqn={subnqn} ns=1' "
            f"--numjobs=1 --thread=1 {args} "
        )
        return command

    def open(self):
        print(f"{self.build()}")
        return subprocess.Popen(self.build(), shell=True)

    def run(self):
        print(f"{self.build()}")
        try:
            return subprocess.run(self.build(), shell=True, check=True)
        except subprocess.CalledProcessError:
            if self.userspace:
                self.remove_stale_files_on_error()
            raise

    def remove_stale_files_on_error(self):
        ip = self.uri.hostname
        nqn = self.uri.path[1:]
        root = os.environ["ROOT_DIR"]

        try:
            command = f"rm -f {root}/'trtype=tcp adrfam=IPv4 traddr={ip} trsvcid=8420 subnqn={nqn} ns=1'"
            subprocess.run(command, shell=True, check=True)
        except subprocess.CalledProcessError:
            assert False, "Could not clean up the stale files, needs manual cleanup"
