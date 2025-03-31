import os
import sys
import time

import docker
import pytest

# Constants
ORACLE_PORT = 1521
ORACLE_PWD = "aintnobodygottimefordat"

# Set our path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))


# Function to wait for a specific log message indicating the container is ready
def wait_for_container_log(container, timeout=300, poll_interval=1,
                           ready_message="Completed: Pluggable database FREEPDB1 opened read write"):
    start_time = time.time()
    while time.time() - start_time < timeout:
        # Get the logs from the container
        logs = container.logs().decode('utf-8')

        # Check if the ready message is in the logs
        if ready_message in logs:
            return True

        # Wait for the next poll
        time.sleep(poll_interval)

    raise TimeoutError(f"Container did not show '{ready_message}' in logs within {timeout} seconds.")


@pytest.fixture(scope="session")
def oracle_server():
    client = docker.from_env()
    container = client.containers.run(
        image="container-registry.oracle.com/database/free:latest-lite",
        name="oracle-parquet-dumper-test",
        detach=True,
        remove=True,
        tty=True,
        init=True,
        ports={f"{ORACLE_PORT}/tcp": ORACLE_PORT},
        environment={"ORACLE_PWD": ORACLE_PWD,
                     "ENABLE_ARCHIVELOG": "false",
                     "ENABLE_FORCE_LOGGING": "false"
                     },
        stdout=True,
        stderr=True
    )

    # Wait for the container to be ready
    wait_for_container_log(container)

    yield container

    container.stop()
