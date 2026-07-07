import os
import re
from datetime import datetime

import docker


class Docker(object):
    logged_tests = set()

    # Determines if a container with the given name is running.
    @staticmethod
    def check_container_running(container_name):
        docker_client = docker.from_env()
        try:
            container = docker_client.containers.get(container_name)
        except docker.errors.NotFound as exc:
            raise Exception("{} container not found", container_name)
        else:
            container_state = container.attrs["State"]
            if container_state["Status"] != "running":
                raise Exception("{} container not running", container_name)

    # Get the status of the container with the given name
    @staticmethod
    def container_status(container_name):
        docker_client = docker.from_env()
        try:
            container = docker_client.containers.get(container_name)
        except docker.errors.NotFound as exc:
            raise Exception("{} container not found", container_name)
        else:
            container_state = container.attrs["State"]
            return container_state["Status"]

    @staticmethod
    def container_ip(container_name):
        docker_client = docker.from_env()
        try:
            container = docker_client.containers.get(container_name)
        except docker.errors.NotFound as exc:
            raise Exception("{} container not found", container_name)
        else:
            return container.attrs["NetworkSettings"]["Networks"]["cluster"][
                "IPAddress"
            ]

    # Kill a container with the given name.
    @staticmethod
    def kill_container(name):
        docker_client = docker.from_env()
        container = docker_client.containers.get(name)
        container.kill()

    # Stop a container with the given name.
    @staticmethod
    def stop_container(name):
        docker_client = docker.from_env()
        container = docker_client.containers.get(name)
        container.stop()

    # Pause a container with the given name.
    @staticmethod
    def pause_container(name):
        docker_client = docker.from_env()
        container = docker_client.containers.get(name)
        container.pause()

    # Unpause a container with the given name.
    @staticmethod
    def unpause_container(name):
        docker_client = docker.from_env()
        container = docker_client.containers.get(name)
        container.unpause()

    @staticmethod
    def execute(name, commands):
        docker_client = docker.from_env()
        container = docker_client.containers.get(name)
        return container.exec_run(commands)

    # Restart a container with the given name.
    def restart_container(name):
        docker_client = docker.from_env()
        container = docker_client.containers.get(name)
        container.restart()

    @staticmethod
    def log_containers():
        failed_logs_var = "FAILED_DOCKER_LOGS"
        ci = "CI"
        logs = None
        current_test = os.environ.get("PYTEST_CURRENT_TEST")

        match_logs = re.match(
            r"^(.*?)\.py::([^\s]+)(?:\s+\((setup|call|teardown)\))?$", current_test
        )
        if not match_logs:
            print(f"No match for test file and name in current_test: {current_test}")
            return

        print(f"Dumping container logs for test: {current_test}")

        if failed_logs_var in os.environ and os.environ[failed_logs_var]:
            logs = os.environ.get(failed_logs_var)
            print(f"Dumping container logs to: {logs}")
        elif ci in os.environ and os.environ[ci] in ["1", "True", "true"]:
            logs = os.path.join(
                os.environ.get("ROOT_DIR"), "ci-report", "docker-logs.txt"
            )
            print(f"Dumping container logs to: {logs}")

        if logs is None:
            print("No log file specified for docker logs. Skipping log dump.")
            return

        match_logs = re.match(
            r"^(.*?)\.py::([^\s]+)(?:\s+\((setup|call|teardown)\))?$", current_test
        )
        if match_logs:
            test_file, test_name, action = match_logs.groups()
            Docker.dump_logs_single(logs, current_test)
            Docker.dump_logs_multi(os.path.dirname(logs), test_file, test_name, action)
        else:
            print(f"No match for test file and name in current_test: {current_test}")
            Docker.dump_logs_single(logs, current_test)

    @staticmethod
    def dump_logs_multi(dump_path: str, test_file: str, test_name: str, action: str):
        docker_client = docker.from_env()

        for container in docker_client.containers.list():
            file = os.path.join(
                dump_path, f"{test_file}/{test_name}/{action}/{container.name}.txt"
            )
            os.makedirs(os.path.dirname(file), exist_ok=True)
            with open(file, "w") as log_file:
                container_logs = container.logs().decode("utf-8")
                log_file.write(container_logs)

    @staticmethod
    def dump_logs_single(dump_path: str, current_test: str):
        docker_client = docker.from_env()

        with open(dump_path, "a") as log_file:
            log_file.write(f"{datetime.now()}: Logs for Test {current_test}:\n")
            log_file.write("-" * 40 + "\n\n")
            for container in docker_client.containers.list():
                log_file.write(f"Logs for container {container.name}:\n")
                log_file.write("-" * 40 + "\n")
                logs = container.logs().decode("utf-8")
                log_file.write(logs)
                log_file.write("\n\n")
            log_file.write(f"End of Logs for Test {current_test}:\n")
            log_file.write("-" * 40 + "\n\n\n")
