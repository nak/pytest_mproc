"""
Tests that invoke pytest as a subprocess to test
(1) local, no parallel (that basic serialized pytest works as usual)
(2) local, with multipler cores (to test local parallelized runs on one host
(3) If enabled, distributed execution across multiple hosts
"""
import secrets
import subprocess
import sys
import time

from junitparser import  JUnitXml
from pathlib import Path

from pytest_mproc import _get_my_ip, _find_free_port

RESOURCE_TEST_PATH = Path(__file__).parent / 'resources' / 'project_tests'


def test_local_serailized(monkeypatch):
    monkeypatch.chdir(RESOURCE_TEST_PATH)
    cmd = "pytest -s . --loop 5 --junitxml=pytest_report.xml"
    proc = subprocess.Popen(cmd, shell=True, stdout=sys.stdout, stderr=sys.stderr)
    proc.wait(timeout=10)
    assert proc.returncode == 1
    for test_suite in JUnitXml.fromfile("pytest_report.xml"):
        assert test_suite.failures == 5
        assert test_suite.tests == 10


def test_local_parallelized(monkeypatch):
    monkeypatch.chdir(RESOURCE_TEST_PATH)
    cmd = "pytest -s . --loop 20 --junitxml=pytest_report.xml --cores 4"
    proc = subprocess.Popen(cmd, shell=True, stdout=sys.stdout, stderr=sys.stderr)
    proc.wait(timeout=20)
    if proc.returncode is None:
        proc.kill()
    for test_suite in JUnitXml.fromfile("pytest_report.xml"):
        assert test_suite.failures == 20
        assert test_suite.tests == 40
    assert proc.returncode == 1


def test_local_distributed(monkeypatch):
    # tests distributed capability but uses ip of host to distribute worker agents (aka no external nodes)
    monkeypatch.chdir(RESOURCE_TEST_PATH)
    authkey = secrets.token_bytes(64)
    cmd = f"{sys.executable} -m pytest_mproc.worker"
    agent_proc = subprocess.Popen(cmd, shell=True, stdin=subprocess.PIPE, stderr=subprocess.PIPE, stdout=sys.stdout)
    agent_proc.stdin.write(authkey.hex().encode('utf-8') + b'\n')
    agent_proc.stdin.close()
    try:
        port_img = agent_proc.stderr.readline()
        port = int(port_img)
        with open('workers.txt', 'w') as out_stream:
            out_stream.write(f'authkey:{authkey.hex()}\n')
            ip = _get_my_ip()
            if ip is None:
                raise RuntimeError("Unable to determine host ip")
            for _ in range(8):
                out_stream.write(f"{ip}:{port}\n")
        cmd = "pytest -s . --loop 20 --junitxml=pytest_report.xml --cores 8 --distributed fixed_hosts://workers.txt"
        print(f"LAUNCH {cmd}")
        proc = subprocess.Popen(cmd, shell=True, stdout=sys.stdout, stderr=sys.stderr)
        proc.wait(timeout=120)
        if proc.returncode is None:
            proc.kill()
        for test_suite in JUnitXml.fromfile("pytest_report.xml"):
            assert test_suite.failures == 20
            assert test_suite.tests == 40
        assert proc.returncode == 1
    finally:
        agent_proc.kill()


def test_local_distributed_failed_workers(monkeypatch):
    # tests distributed capability but uses ip of host to distribute worker agents (aka no external nodes)
    # THis is off-nominal as we do not start worker agents and no workers are started, testing that process
    # end in reasonable amount of time and doesn't hang on this condition
    monkeypatch.chdir(RESOURCE_TEST_PATH)
    authkey = secrets.token_bytes(64)
    with open('workers.txt', 'w') as out_stream:
        out_stream.write(f'authkey:{authkey.hex()}\n')
        ip = _get_my_ip()
        if ip is None:
            raise RuntimeError("Unable to determine host ip")
        port = _find_free_port()
        for _ in range(4):
            out_stream.write(f"{ip}:{port}\n")
    cmd = "pytest -s . --loop 20 --junitxml=pytest_report.xml --cores 4 --distributed fixed_hosts://workers.txt"\
          " --full-trace"
    proc = subprocess.Popen(cmd, shell=True, stdout=sys.stdout, stderr=sys.stderr)
    start = time.monotonic()
    proc.wait(timeout=120)
    assert time.monotonic() - start < 20,  "pytest failed to exit in time under condition no workers started"
    if proc.returncode is None:
        proc.kill()
    for test_suite in JUnitXml.fromfile("pytest_report.xml"):
        assert test_suite.failures == 0
        assert test_suite.tests == 0
    assert proc.returncode != 0
