import sys

import pytest
from _pytest.config import Config

from pytest_mproc import user_output
from pytest_mproc.remote.data import Settings
from pytest_mproc.utils import BasicReporter, is_degraded


class TestSession:

    def __init__(self, config: Config):
            """
            Called before "true" main routine.  This is to set up config values well ahead of time
            for things like pytest-cov that needs to know we are running distributed
            """
            reporter = BasicReporter()
            config.ptmproc_test_session = self
            username = config.getini('ssh_username')
            if username is not None:
                Settings.set_ssh_credentials(username, None)
            self._mproc_disabled = config.getoption("mproc_disabled")
            self._num_cores = config.getoption("mproc_numcores")
            if config.getoption("mproc_verbose") is True:
                user_output.is_verbose = config.option.mproc_verbose
            if self._num_cores is None or is_degraded() or self._mproc_disabled:
                if '--as-main' in sys.argv or '--as-worker-node' in sys.argv:
                    raise pytest.UsageError(
                        "Refusing to execute on distributed system without also specifying the number of cores to use")
                if self._num_cores:
                    reporter.write(
                        ">>> no number of cores provided or running in environment that does not support parallelized testing, "
                        "not running multiprocessing <<<<<\n", yellow=True)
            elif self._num_cores is not None and self._num_cores <= 0:
                raise pytest.UsageError(f"Invalid value for number of cores: {self._num_cores}")
            # tell xdist not to run
            config.option.dist = "no"
            val = config.getvalue
            if not val("collectonly"):
                # noinspection SpellCheckingInspection
                usepdb = config.getoption("usepdb")  # a core option
                if val("dist") != "no" and usepdb:
                    raise pytest.UsageError(
                        "--pdb is incompatible with distributing tests."
                    )  # noqa: E501
            self._coordinator =
