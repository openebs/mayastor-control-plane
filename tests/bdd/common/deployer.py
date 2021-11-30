import os
import subprocess


class Deployer(object):
    # Start containers with the default arguments.
    @staticmethod
    def start(num_mayastors):
        deployer_path = os.environ["ROOT_DIR"] + "/target/debug/deployer"
        # Start containers and wait for them to become active.
        subprocess.run(
            [
                deployer_path,
                "start",
                "--csi",
                "-j",
                "-m",
                str(num_mayastors),
                "-w",
                "10s",
            ]
        )

    # Start containers with the provided arguments.
    @staticmethod
    def start_with_args(args):
        deployer_path = os.environ["ROOT_DIR"] + "/target/debug/deployer"
        subprocess.run([deployer_path, "start"] + args)

    # Stop containers
    @staticmethod
    def stop():
        deployer_path = os.environ["ROOT_DIR"] + "/target/debug/deployer"
        subprocess.run([deployer_path, "stop"])
