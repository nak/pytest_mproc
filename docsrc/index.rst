Welcome to pytest_mproc's documentation!
=========================================

.. toctree::
   :maxdepth: 2
   :caption: Contents:

   developer-guide


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

Introduction
============

Welcome to *pytest_mproc*, a plugin for pytest to run distributed testing via multiprocessing.  This manner
of distributed testing has several advantages, including more efficient execution over pytest-xdist in many cases.

xdist works for cases perhaps where there are large number of tests, but the cost of overhead in using rsync and
overall implementation prevents realization of test-execution-time gains that one might hope for.

To begin using pytest_mproc, just install:

% pip install pytest-mproc

*pytest_mproc* is usable in the context of a single host with multiple cores, as well as execution across a distributed
set of machines.

Why Use *pytest_mproc* over *pytest-xdist*?
===========================================

*pytest_mproc* has several advantages over xdist, depending on your situation:

* Overhead furing startup is much less, and start-up time does not grow with increasing number of cores as in xdist
* It uses a pull model, so that each worker pulls the next test from a master queue, with no need to figure
  out how to divide the tests beforehand.  The disadvantage is that optimal efficiency requires knowledge to
  prioritize longer running tests first.
* It provides a 'global' scoped test fixture to provide a single instance of a fixture across all tests, regardless of
  how many nodes.
* It provide a 'node' scoped test fixture that is a single instance across all workers on a single machine (node)
* It allows you to programatically group together a bunch of tests to run serially on a single worker process
* Support for execution across multiple machines
* It provides a better mechanism for prioritizing and configuring test execution


Usage
=====

To use with pytest, you use the -C or equivalently --cores argument to specify how many cores to run on:

% pytest --cores 3 [remaining arguments]

or to specify use of all available cores:

% pytest --cores auto [remaining arguments]

You can also specify things like "auto*2" or "auto/2" for number of cores.

.. caution::
   Oversubscribing CPUs will cause additional overhead.  *pytest_mproc* kicks off all worker threads up front, and
   each worker does identical processing to independently come up with the list of tests.  When the number of
   threads is less than or equal to the number of cores ("a batch"), this adds little overhead.  Once beyond that, each
   additional (partial) batch will add more overhead, as the CPU must be split across multiple threads for some or all
   cores.

Disabling *pytest_mproc* from Command Line
==========================================

To disable mproc (overriding all other pytest-mproc-related flags), specify "--diable-mproc True" on the command line.

Top-Level Command Line Configuration Options
============================================

Aside from specifying the number of cores, the user can optionally specify the maximum number of active connections
allowed during distributed processing.  The *multiprocessing* module can get bogged down and dwell on one thread or
even deadlock when too many connection requests are made at one time to the main process.  To alleviate this,
the number of allowed simultaneous connections is throttled, with a default of 24 maximum connections.  The user can
over ride this using the *--max-simultanous-connections* option.


Grouping Tests
==============

By default all tests are assumed independently runnable and isolated from one another.  That is the order in which they
run within which process does not matter.  You may wish to specify a group of tests that must run serially within the
same process/thread.  To do this, annotate each test using a unique name for the group:

.. code-block:: python

   import pytest_mproc
   TEST_GROUP1 = "group1"

   @pytest_mproc.group(TEST_GROUP1)
   def test_something1():
        pass

   @pytest_mproc.group(TEST_GROUP1)
   def test_something2():
        pass

Likewise, you can use the same annotation on test class to group all test methods within that class:

.. code-block:: python

   import pytest_mproc

   TEST_GROUP_CLASS = "class_group"

   @pytest.mproc.group(TEST_GROUP_CLASS)
   class TestClass:
       def test_method1(self):
          pass

       def test_method2(self):
          pass

This is useful if the tests are using a common resource for testing and parallelized execution of tests might
result in interference.

Groups can also be specified as class *pytest_mproc.data.GroupTag* instead of a string, to prioritize the whole group
and restrict the group to running within a single worker or withtin a single node machine:

.. code-block:: python

   import pytest_mproc
   from pytest_mproc import GroupTag, TestExecutionConstraint

   TEST_GROUP_CLASS = GroupTag("class_group", priority=100, restritc_to: TestExecutionConstraint.SINGLE_PROCESS)

   @pytest.mproc.group(TEST_GROUP_CLASS)
   class TestClass:
       def test_method1(self):
          pass

       def test_method2(self):
          pass


Configuring Test Execution of Groups
------------------------------------

As discussed below, *pytest_mproc* allows execution across multiple machines/indepenent processes.  There are two
ways to specify how a group of tests will be executed by using the *restric_to* parameter of the
*pytest_mproc.group* decorator.  This can take one of two values.

#. *pytest_mproc.TestExecutionConstraint.SINGLE_PROCESS* - this is the default value.  This specifies that the tests
   are to run serially based on priority of tests within the group on a single core
#. *pytest_mproc.TestExecutionConstraint.SINGLE_NODE*  - This specifies that the group of tests are to run on a single
   node of the system, but within that node the set of tests can be distributed among the workers of that node

If specifying *SINGLE_NODE* execution in the context of running a single *pytest* process (and therefore a single node),
the only value provided is the ability to specify a hierarchy of priorities.


A Matter of Priority
====================
You can also specify a priority for test execution using the *pytest_mproc.priority* decorator:

.. code-block:: python

   import pytest_mproc

   @pytest.mpro.priority(pytest_mproc.DEFAULT_PRIORITY - 2)
   def test_function():
      pass

Groups with lower interger priority value are scheduled before those with a higher priority value.  Thus, a
group with priority *pytest_mproc.DEFAULT_PRIORITY-1* will be scheduled before a group with priority
*pytest_mproc.DEFAULT_PRIORITY-2*.  Of course, the default priority of *pytest_mproc.DEFAULT_PRIORITY*  is used if
unspecified.

Priorities should be specified to the default priority assigned to all tests without decorators, which can be
obtained through the variable *pytest_mproc.DEFAULT_PRIORITY*.  When working with grouped tests as explained above,
the priorities are scoped.  That is, at a global level, the priority of a group of tests will determine that group's
execution order relative to all other top-level tests or test groups. For tests within a group, the priority of each test
will determine the order of execution only within that group.  *pytest_mproc* does not guarantee order of
execution for tests or test groups with the same priority.

If you specify a priorty on a class of tests, all test methods within that class will have that priority, unless
a specific priority is assigned to that method through its own decorator.  In that class, the decorator for the class
method is used.


A 'global'-ly Scoped Fixture
============================

As is the case with *pytest_xdist* plugin, *pytest_mproc* uses multiprocessing module to achieve parallel concurrency.
This raises interesting behaviors about 'session' scoped fixtures.  Each subprocess that is launched will create its own
session-level fixture;  in other words, you can think of a session as a per-process concept, with one session-level
fixture per process.  This is often not obvious to test developers. Many times, a global fixture is needed, with a
single instantiation across all processes and tests.  An example might be setting up a single test database.
(NOTE: when running on more than one machine, also see 'node-level' fixtures discussed below)

To achieve this, *pytest_mproc* adds a new scope: 'global'. Behind the scenes, the system uses pythons *multiprocessing*
module's BaseManager to provide this feature.  This is mostly transparent to the developer, with one notable exception.
In order to communicate a fixture globally across multiple *Process* es, the object returned (or yielded) from a
globally scoped fixture must be 'picklable'.

The 'global' scope is a level above 'session' in the hierarchy.  That means that globally scoped fixtures can only
depend on other globally scoped fixtures.

.. warning::
    Values returned or yielded from a fixture with scope'global' must be picklable so as to be shared across multiple
    independent subprocesses.


.. code-block:: python

   import pytest

   @pytest_mproc.fixtures.global_fixture(host="some.host", port=<some port, or None to pick a random free port>)
   def globally_scoped_fixture()
       ...

Global fixtures can be passed any keywoard args supported by pytest.fixure.

Tips for Using Global Scope Fixture
-----------------------------------
Admittedly, one has to think about how multiprocessing works to know what can and cannot be returned from a global
test fixture.  Since they are "singletons", one useful tip if you are using (singleton) classes (such as manager classses) to
encaspulate logic, it is best not to put anything other than what is necessary as instance variables, and delegate
them to class variable.  Also, you should design your fixture so that any other test not depending on it is not blocked
while on that fixture.  Example code is shown blow


.. code-block:: python

   import pytest
   import multiprocessing

   class Manager:

        # not picklable, so keep at class level
       _proc: multiprocessing.Process = None

       @staticmethod
       def start(hostname: str, port: int) -> Nont:
           ...
           start_some_service(hostname, port)

       @classmethod
       def setup_resources(cls) -> Nont:
            hostname, port = '127.0.0.1', 32873
            # service is started i the background so as not to hold up the system
            cls._proc = multiprocessing.Procss(target=start, args=(hostname, port))
           ...
           return hostname, port

       @classmethod
       def shutdown(cls, hostname:str , port: int) -> None:
            ...

       def __init__(self):
           assert self._proc is not None, "Attempt top instantiate singleton more then once"
           # hostname and port are a str and int so easily Picklable
           self._hostname, self._port = self.setup_resources()

       def __enter__() -> "Manager":
           return self

       def __exit__(self, *args, **kargs):
           self.shudown(self._hostname, self._port)
           self._proc.join(timeout=5)

       def hostname(self):
            return self._hostname

        def port(self):
            return self._port


    @pytest.fixture(scope='global')
    def resource():
        # called only once across all processes and for all tests
        with Manager() as manager:
            yield manager.hostname, manager.port


*pytest_mproc* guarantees that all fixtures are in place before worker clients in other processes access them, so as
not to cause a race condition.

Node-scoped Fixtures
====================

With the system now allowing execution of mutliple workers on a single machine, and execution across multiple machines,
this introduces a new level of scoping of fixtures:  *node*-scoped fixtures.  A fixture scoped to '*node*' is
instantiated once per node and such instance is only available to workers on that node.

To declare a fixture to be scoped to a node:

.. code-block:: python

   import pytest

   @pytest_mproc.fixtures.node_fixture(autouse=True)
   def globally_scoped_fixture()
       ...

Node fixtures can be passed any keywoard args supported by pytest.fixure.

.. warning::
    Values returned or yielded from a fixture with scope to a node must be picklable so as to be shared across multiple
    independent subprocesses.

A Safe *tmpdir* Fixture
=======================

*pytest_mproc* provides its own fixtures for creating temporary directories: *mp_tmpdir_factory*, a fixture with a
"*create_tmpdir*" method for creating multiple temporary directories within a test procedure, and a
*mp_tmpdir* directory for creating a single temporary directory.  Other plugins providing similar featurs can have
race conditions causing intermittent (if infrequent) failures.  This plugin guarantees to be free of race conditions
and safe in the context of *pytest_mproc*'s concrurrent test framework.

(the regular pytest tmpdir and corresponding factory fixtures have a race condition that can cause a tmp dir to
be removed that is in use by another thread worker in any distributed execution)

Advanced: Testing on a Distributed Set of Machines
==================================================

*pytest_mproc* has support for running test distributed across multiple mahcines.

.. caution::
   This feature may not be as robust against fault tolerance.  The basic functionality exists, but robustness
   against exception in fixtures, etc., has not been fully tested

.. caution::
   Throttling of connections only happens on a per-node basis, not globally.  It may be possible to overwhelm
   the main node if you are using too many remote workers, causing deadlock

The concept is that one node will act as the main node, collecting and reporting test status, while all others
will act as clients -- executing tests and reporting status back the the main node.  To start the main node,
use the "--as-main" command line argument, specifying the port the server will listen on:

.. code-block:: shell

   % pytest --num_cores <N> --as-main <protocol><host>:<port> ...

When *--as-main* argument is present, N represents the number of cores to use on each worker when
automated distributed testing is invoked, unless overridden
by remote-config data specific to a worker.  This sets up the node as main, responsible for orchestrating test
execution across multiple workers.  If no other options are present, workers must be started manually.  See
section on automated distributed testing below for a less manual option.

The argument to as-main can take several forms (only one of which is useful for manual deployment of workers.  The
*multiprocessing* package uses a server for handling remote procedure calls, and this option specifies how
to start or connect to that server The possible options are:

* *[local://]<host>:<port>* -- host should be an ip address associated with local host and port is an available port
* *[remote://]<host>:<port>* -- the host is manually (possibly continually) running on a remote machine given by the host
and port of that server
* *[ssh://][user@]<host>:<port> -- start a host automatically via ssh on a remote machine specified by host and port (with
optional user specification)

To start the client nodes:

.. code-block:: shell

   % pytest --num_cores <N> --as-worker <host>:<port> [--connection-timeout <integer seconds>]

Here, N must be greater than or equal to 1;  multiple workers can be invoked on a node, with possibly multiple client
nodes. The client will attempt to connect to the main  node for a period of 30 seconds at which point it gives up and
exits with na error.  If *--connection-timeout* is specified, the client will timeout and exit if it takes longer than
the provided seconds to connect.  The default is 30 seconds.

Automate Distributed Execution
------------------------------

*pytest_mproc* supports automated deployment and execution on a set of remote worker machines.  This requires a method
to determine the remote hosts (and parameters associated with those hosts as needd) as well as a project strcuture to
bundle and deploy to remote host machines.  Deployments and execution are conducted vis *ssh* and any port
referenced in options is the *ssh* port to be used.

Project definition consists of defining a set of properties in a config file in JSON format:

.. code-block:: json

   {
    "src_paths": ["./path/to/src1", "./path/to/src2"],
    "test_files": "./path/to/project_tests/*.pattern",
   }

These have the following meanings for creating the bundle to send to workers:

* *src_paths*: list of directories or evein site-package directories to include in the bundle
* *test_files*: location of where test code is kept, to be bundled as as directory;  this can specify any files
and are copied to remote hosts using a path relative to the location of the project config file.  These files
can include requirements.txt files, resources files and of course test code (but really any type of file)
* *prepare_script*: an optional script that is run before pytest execution on a remote host, to prep the
  test environment if needed.  The recommendation is to make use of pytest fixtures, however.
* *finalize_script*: an optional script that is run after pytest execution on a remote host, to tear down the
  test environment if needed.  The recommendation is to make use of pytest fixtures, however.

Project configurations are specified in a file with the path to that file specified on the command line via
the *--project_structure* command line option, or if not provided and *project.cfg" file exists in the current
working directory of test execution, that file will be used.  If you specify a remote hosts configuration
for automated distribute execution, the project conbifuration file is required to tell *pytest_mproc* how
to bundle the necessary items to send to the remote hosts.

The remote hosts configuration is specified on the command line through the *--remote-worker* command line option.
The value of this option can be

* a fixed host specification in the form of "<host>[:<port>];optoin1=value1;...";  the option/value pair can be
  repeated to specify multiple remote clients
* a file with line-by-line JSON content specifying the remote host/port and options as specified below
* an http end point that returns the remote host/port and options, again in line-by-line format

When using an http endpoint, *pytest_mproc* will do lazy loading of the response.  This is to cover the case where
the request to the endpoint is based on a reservation system to reserve resources.  The resource may require
preparation time before returning a reserved host or resrouce back to the main host, or may only be able to
return a subset of the requested resources until the other become available.  In this way, *pytest_mproc* can
start execution on the available resources while waiting for the others to become available (optimized
execution strategy).

The format of the http response or in the file is in a line-by-line JSON format.  For example, if I am using pytest
to drive execution from remote hosts against attached Android devices, the content might look like:

.. code-block:: json

   {"host": "host.name.or.ip", "arguments": {"device": "ANDROID_DEVICE1", "argument1": "value1"}}

Arguments that are all upper case names will be passed to the remote host as environment variables.  All others will
be passed to pytest as command line options in the form "--<option> <value>" and your pytest must be configured
to add and accept those command line options.  There are a few command line options that are reserved for use
by *pytest_mproc*:
* *cores* : specifies the number of cores (worker threads) to instantiate on the remote host;  if not specified
the number of cores specified on the main host command line will be used (see below)
* *jump_host* : if specified, this host:port pair will be used to add a jump host option when invoking ssh against
the remote host

Distributed parallel execution from the command line can then be done through a single invocation:

.. code-block:: bash

    % pytest --as-main", <server_host>:<server_port> --cores 1 -k alg2 --project_structure path/to/project.cfg --remote-worker https://some.endpoint.com/reserver?count=5"

Note that pytest options not specific to pytest_mproc itself are passed along to the client workers (in this case,
the "-k ale2" is pass along).  The "--cores" option is also passed to the worker client and ignores from the
main server process (aka the main server process that is launched through this command will not execute any
worker threads, leaving all work up to the remote clients).  If "cores" is a part of the options provided for
a remote host as part of its configuration (as described above), it will override the value from this command line
invocation.

Breaking this down, lets say we are testing against Android devices on remote hosts and the https endpoint returns:

.. code-block:: json

    {"host": "host.name.or.ip1", "arguments": {"DEVICE_SERIAL": "Z02348FHJ", "default_orientation": "landscape"}}
    {"host": "host.name.or.ip2", "arguments": {"DEVICE_SERIAL": "Z0B983HH1", "default_orientation": "portrait"}}

Then the above command on the server will:

#. collect all test cases to be executed (pytest functionality)
#. populate a test queue for workers to pull from with these cases
#. in parallel, bundle, deploy and execute pytest on the worker threads

Each worker host will:

#. collect all test cases to be executed (pytest functionality)
#. have DEVICE_SERIAL available in *os.environ* during execution
#. be provided the option *default_orientation* as given above
#. launch with a single worker thread (cores = 1) providing a 1-to-1 worker-host-to-device
#. each worker thread start a test loop that pulls each test from the main test queue and executes each test until
   the queue is exhausted

If --cores were specified as 4, then each worker host would launch with 4 parallel worker threads, unless
the https endpoint provided a "cores" option with a different value as an override.

Configuration
-------------

Global and node scoped fixtures are maintained through a multirprocessing manager, and require assignemnt of a
port.  Normally, the main process will find a free port (random selection) to use.  This behavior can be
overridden by setting *PTMPROC_NODE_MGR_PORT* and *PTMPROC_GLOBALMGR_PORT* envorionment variables to specific
port integger values.

Setting *PTMPROC_VERBSE* to '1' will spit out a lot of information about whay steps *pytest_mproc* is taking
to execute.

Logs and Artifacts
------------------

*pytest_mproc* will collect logs from remote workers and pull hem back to the main host.  By default,
these are collected on the main host in "./artifacts" when running, but the location can be overriden by setting the
environment variable PTMPROC_HOST_ARTIFACTS_DIR.

On the worker nodes (when runing distributed automatically), the environment variable PTMPROC_WORKER_ARTIFACTS_DIR
is set to tell pytest where to find the artifacts dir (absolute path) on the worker host.
Setting this environment variable will
have no effect, it is only read in at the beginning of the program. In other words, the location for workers
is fixed by the program and cannot be changed, and should be used as the top-level
location to store (or copy) artifacts.  Any artifacts collected under this directory will be zipped and pulled
back to:

.. code-block:: bash

   $(PTMPROC_HOST_ARTIFACTS_DIR)/Worker-<n>/artifacts.zip

If not set, "./artifacts" is used for PTMPROC_HOST_ARTIFACTS_DIR.
