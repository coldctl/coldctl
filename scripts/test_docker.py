"""Disposable Docker lifecycle acceptance: python scripts/test_docker.py IMAGE."""
import json
import subprocess
import sys
import time
import uuid
from pathlib import Path


def docker(*args, check=True):
    result = subprocess.run(["docker", *args], capture_output=True, text=True, timeout=180)
    if check and result.returncode:
        raise RuntimeError(result.stderr + result.stdout)
    return result


def main():
    image = sys.argv[1]
    deployment = Path(__file__).resolve().parents[1] / "deploy/docker"
    compose = json.loads(docker("compose", "--env-file", str(deployment / ".env.example"),
                                "-f", str(deployment / "compose.yaml"), "config", "--format", "json").stdout)
    service = compose["services"]["agent"]
    assert service["init"] and service["read_only"] and service["user"] == "10001:10001"
    assert int(service["mem_limit"]) == 2147483648 and float(service["cpus"]) == 2
    assert int(service["pids_limit"]) == 128 and not service.get("ports")
    name = "coldctl-docker-test-" + uuid.uuid4().hex[:12]
    volume = name + "-state"
    docker("volume", "create", volume)
    common = ["--init", "--read-only", "--cap-drop=ALL", "--security-opt=no-new-privileges",
              "--memory=2g", "--memory-swap=2g", "--cpus=2", "--pids-limit=128",
              "--tmpfs", "/tmp:size=268435456,mode=1777,noexec,nosuid,nodev",
              "-v", volume + ":/var/lib/coldctl"]
    try:
        docker("run", "--rm", "--user=0", "--entrypoint=sh", "-v",
               volume + ":/state", image, "-c", "chown 10001:10001 /state")
        docker("run", "--rm", *common, image, "init")
        before = docker("run", "--rm", *common, image, "status").stdout
        assert before.strip(), "status missing"
        docker("run", "--rm", *common, "--entrypoint=sh", image, "-c",
               'test "$(id -u)" = 10001 && ! touch /etc/forbidden && test -f /var/lib/coldctl/state.db')
        for attempt in range(2):
            docker("run", "-d", "--name", name, *common, image)
            time.sleep(3)
            state = json.loads(docker("inspect", name).stdout)[0]
            assert state["State"]["Running"], docker("logs", name).stdout
            assert state["HostConfig"]["Memory"] == 2147483648
            assert state["HostConfig"]["NanoCpus"] == 2000000000
            # A second worker must not run against the same state.
            other = docker("run", "--rm", *common, image, "agent", "scheduler", "run", "--once", check=False)
            assert other.returncode != 0, "second worker was admitted"
            docker("stop", "--time=30", name)
            state = json.loads(docker("inspect", name).stdout)[0]
            assert state["State"]["ExitCode"] == 0, docker("logs", name).stdout
            assert not state["State"]["OOMKilled"]
            docker("rm", name)
        after = docker("run", "--rm", *common, image, "status").stdout
        assert after == before, "state status changed across idle restarts"
        # Invalid filenames fail closed without executing their contents.
        docker("run", "--rm", "--user=0", "--entrypoint=sh", "-v", volume + ":/state", image,
               "-c", "mkdir /state/secrets; printf value > /state/secrets/bad-name")
        bad = docker("run", "--rm", *common, "-v", volume + ":/run/secrets:ro", image, "status", check=False)
        # state.db is itself an invalid env filename in this negative fixture.
        assert bad.returncode != 0 and "Invalid secret filename" in bad.stderr
        print("PASS: non-root/read-only image, durable state, exclusive worker, two SIGTERM restarts, invalid secrets")
    finally:
        docker("rm", "-f", name, check=False)
        docker("volume", "rm", volume, check=False)


if __name__ == "__main__":
    main()
