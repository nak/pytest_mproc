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

To begin using pytest_mproc, just install:

% pip install pytest-mproc

*pytest_mproc* is usable in the context of a single host with multiple cores, as well as execution across a distributed
set of machines.

Why Use *pytest_mproc* over *pytest-xdist*?
===========================================

Why not just use *xdist*, another parallelization mechanism for test execution? *xdist* works for cases perhaps
where there are large number of tests, but the cost of overhead in using rsync and
overall implementation prevents realization of test-execution-time gains that one might hope for.  *xdist* also
does not provide a way to run distributed on multiple machines

*pytest_mproc* has several advantages over xdist, depending on your situation:

* Overhead firing startup is much less, and start-up time does not grow with increasing number of cores as in xdist
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

You can also specify things like "auto*2" or "auto/2" for number of cores.  This is all that is needed
to get started with the minimal configuration for *pytest_mproc* -- running tests on multiple cores on a single machine.

.. caution::
   Oversubscribing CPUs will cause additional overhead.  *pytest_mproc* kicks off all worker threads up front, and
   each worker does identical processing to independently come up with the list of tests.  When the number of
   threads is less than or equal to the number of cores ("a batch"), this adds little overhead.  Once beyond that, each
   additional (partial) batch will add more overhead, as the CPU must be split across multiple threads for some or all
   cores.

When going beyond the simple one-machin parallelization, certain terminology is used.  The term 'node' is used to
refer to a single machine within a group of machines (over which execution is distributed).  The term 'main node'
refers to the orchestrator of the tests, responsible for overseeing test execution and reporting results to the
user.  The term 'worker' is used to specify a single process running on a node that is doing the actual test
execution.  The main node can distrbitue tests acros multiple other nodes, with multiple worker processes executing
within each node.

Disabling *pytest_mproc* from Command Line
------------------------------------------

To disable mproc (overriding all other pytest-mproc-related flags), specify "--diable-mproc True" on the command line.
Running without the --cores option will also run as normal, with no invocations of pytest_mproc features.


Grouping Tests
==============

By default all tests are assumed independently runnable and isolated from one another, and the order in which they
run within which process does not matter.  To group tests that must run serially within the
same process/thread, annotate each test using a unique name for the group:

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

As discussed below, *pytest_mproc* allows execution across multiple machines/independent processes.  There are two
ways to specify how a group of tests will be executed by using the *restrict_to* parameter of the
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

   @pytest_mproc.priority(pytest_mproc.DEFAULT_PRIORITY - 2)
   def test_function():
      pass

Groups with lower integer priority value are scheduled before those with a higher priority value.  Thus, a
group with priority *pytest_mproc.DEFAULT_PRIORITY-1* will be scheduled before a group with priority
*pytest_mproc.DEFAULT_PRIORITY-2*.  Of course, the default priority of *pytest_mproc.DEFAULT_PRIORITY*  is used if
unspecified.

Priorities should be specified to the default priority assigned to all tests without decorators, which can be
obtained through the variable *pytest_mproc.DEFAULT_PRIORITY*.  When working with grouped tests as explained above,
the priorities are scoped.  That is, at a global level, the priority of a group of tests will determine that group's
execution order relative to all other top-level tests or test groups. For tests within a group, the priority of each test
will determine the order of execution only within that group.  *pytest_mproc* does not guarantee order of
execution for tests or test groups with the same priority.

If you specify a priority on a class of tests, all test methods within that class will have that priority, unless
a specific priority is assigned to that method through its own decorator.  In that class, the decorator for the class
method is used.


Globally Scoped Fixtures
========================

*pytest_mproc* uses multiprocessing module to achieve parallel concurrency.
This raises interesting behaviors when using *pytest* and 'session' scoped fixtures.
For multiple-process execution, Each subprocess that is launched will create its own
session-level fixture;  in other words,  a session in the context of *pytest* is a per-process concept,
with one session-level fixture for each process.  'session' becomes a misnomer in *pytest* in this way as the term
was not conceived with parallelization in mind.  This is often not obvious to test developers.
At times, a global fixture is needed, with a
single instantiation across all processes and tests.  An example might be setting up a single test database.
(NOTE: when running on more than one machine, also see 'node-level' fixtures discussed below)

To achieve this, *pytest_mproc* adds a 'global' scope feature, instantiated only once and shared with all nodes.
Behind the scenes, the system uses pythons *multiprocessing*
module's BaseManager to provide this feature.  This is mostly transparent to the developer, with one notable exception.
In order to communicate a fixture globally across multiple *Process* es, the object returned (or yielded) from a
globally scoped fixture must be 'picklable'.  To declare a fixture global:

.. code-block:: python

   import pytest

   @pytest_mproc.fixtures.global_fixture(host="some.host", port=<some port, or None to pick a random free port>)
   def globally_scoped_fixture()
       ...

The global fixture is the highest level above 'session' in the hierarchy.  That means that globally scoped fixtures can only
depend on other globally scoped fixtures.

.. warning::
    Values returned or yielded from a global fixture must be picklable so as to be shared across multiple
    independent subprocesses.

Global fixtures can be passed any keyword args supported by *pytest.fixture* EXCEPT autouse.

*pytest_mproc* guarantees that all fixtures are in place before worker clients in other processes access them, so as
not to cause a race condition.

Node-scoped Fixtures
====================

With the system now allowing execution of multiple workers on a single machine, and execution across multiple machines,
this introduces another level of scoping of fixtures:  *node*-scoped fixtures.  A node-level fixture is
instantiated once per node and such instance is only available to workers (aka all test processes) on that specific node.

To declare a fixture to be scoped to a node, which declares the optional autouse:

.. code-block:: python

   import pytest

   @pytest_mproc.fixtures.node_fixture(autouse=True)
   def globally_scoped_fixture()
       ...

Node fixtures can be passed any keyword args supported by pytest.fixure.

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

*pytest_mproc* has support for running test distributed across multiple machines.

.. caution::
   You should validate that your test code does not have any top-level errors before running distributed,
   as common errors will be replicated and shown in the output for all workers, potentially overwhelming the log.

The concept is that one node will act as the main node, which is what the user launches to start the tests. That
node populates the queue of tets, and collects and reports test status. The other nodes are worker nodes (either
launched automatically or manually) and are responsible for pull tests  (or batches of tests explicitly grouped)
one-by-one until the test queue is exhausted, executing each test as it is pulled, returning the status of the test
back to the main node.  When running distributed, the number of cores specified is the number of cores *per worker*.
A worker is a single process that is pulling the tests, executing each one and reporting status.

Complexities of Distributed Execution
-------------------------------------

*pytest_mproc* requires a common server to handle the creation of a test and result queue and control elements, used
to orchestrate test execution.  The machine that this server runs on must allow incoming connections.  There
are options for where this server can run.  The node hosting the main process (where the tester kicks off the
tests) is indeed one such option. However, for a tester running on a laptop testing across a cloud of device
(as an example), it is unlikely that the nodes in the cloud will be able to call into the tester's laptop.  Firewall
restrictions will undoubtedly prevent this.   Understand the nature of the system you are using for
distributed testing.

Automated distributed testing, *pytest_mproc* uses ssh to deploy and execute remote worker tasks.  The test infrastructure
must therefore provide ssh access (passwordless) into the remote nodes.  A configuration file must also be defined
to define the structure of the bundle to be deployed to the worker nodes (discussed later).

Inter-process communication is secure and requires a common authentication token.  For manual deployment to workers,
this token must be shared across all nodes.  Such sharing is up to the user to implement and can be achieved through a
callback to retrieve the authentication token:

.. code-block:: python
   pytest_mproc. set_user_defined_auth_token(callback)
   # here, callback takes no parameters and returns a bytes object containing the auth token


This should be called globally, presumably close to the top of your *conftest.py* file.  If you with not to use
a fixed port for the orchestration server, the tester can also provide a callback for allocating ports (otherwise
if not port is specified, a random free port will be allocated):

.. code-block:: python
   pytest_mproc.set_user_defined_port_alloc(callback)
   # here, callback takes no parameters and returns an int which is the port to use


Manual Distributed Execution
----------------------------
This section documents how to bring up the main node, and then manually bring up workers on (presumably) remote
nodes.

To start the main node, use the "--as-main" command line argument, specifying the port the server will listen on:

.. code-block:: shell

   % pytest --as-main <host>:<port> ...

When *--as-main* argument is present, N represents the number of cores to use on each worker when
automated distributed testing is invoked.  This sets up the node as main, responsible for orchestrating test
execution across multiple workers. In this scenario, workers must be started manually.  See
section on automated distributed testing below if desired to run from a single *pytest* invocation.  In this
scenario, specifying *--num-cores* here will allow workers to also start on the local node, otherwise no workers
will be invoked under this command (delegated to manual bring-up on other machines/processes)

The host an port specified may also be on a remote machine (see section above on why such a scenario might be needed).
In this case, the orchestration manager must be started manually as well.  The tester can create an application
that calls the main function to start the server:

.. code-block:: python
   pytest_mproc.orchestration.main(host, port, auth_key)


To start each the worker node, assuming the necessary deployment of test code has been made on the node:

.. code-block:: shell

   % pytest --num_cores <N> --as-worker <host>:<port>

Here, N must be greater than or equal to 1;  multiple workers can be invoked on a single node.
Of course,  multiple worker nodes can be used as well. Each worker will attempt to connect
to the main  node for a period of 30 seconds at which point it gives up and exits with na error.

Automate Distributed Execution
==============================

*pytest_mproc* supports automated deployment and execution on a set of remote worker machines.  This requires a method
to determine the remote hosts (and parameters associated with those hosts as needd) as well as a definition
of the project structure to bundle and deploy to remote host machines.
Deployments and execution are conducted vis *ssh* and any port referenced in options is the *ssh* port to be used.

One-time Setup
--------------

Project definition consists of defining a set of properties in a config file in JSON format:

.. code-block:: json

   {
    "project_name": "A_name_associated_with_the_project_under_test",
    "test_files": "./path/to/project_tests/*.pattern",
    "prepare_script": "./path/to/test/setup/script",
    "finalize_script": "./path/to/test/teardown/script",
   }

These have the following meanings for creating the bundle to send to workers:

* *test_files*: Location of all files to be bundled necessary to execute tests.  If a *requirements.txt*
  file is present, these requirements will be instsalled via *pip*. All files must be relative paths,
  relative to the project config file itself
* *prepare_script*: an optional script that is run before pytest execution on a remote host, to prep the
  test environment if needed.  The recommendation is to make use of pytest fixtures, however.
* *finalize_script*: an optional script that is run after pytest execution on a remote host, to tear down the
  test environment if needed.  The recommendation is to make use of pytest fixtures, however.

The information provided must be complete to run tests in isolation, without reference to any other local files as
such files will most likely not be available on remote nodes.   Note that any requirements that are installed are
cached so that after first execution, the overhead of *pip install* will be minimal.

Project configurations are specified in a file with the path to that file specified on the command line via
the *--project_structure* command line option.  Optionally, if not provided and *ptmproc_project.cfg" file
exists in the current working directory of test execution, that file will be used.

.. caution::
   All paths specified in the project configuration file must be relative to the configuration file itself,
   and should be at or below the directory level of the configuration file

Any SSH configuration can be specified programmatically if desired.  This allows for a common ssh username or
credentials specification that is common to remote nodes, so that they need not be specified repeatedly on
command line, for example.  Passwordless ssh with proper authentication key set up is preferred, although a
mechanism to retrieve secrets securely, such as an ssh password, works as well.  The following python functions
provide the API for SSH configuration:

.. code-block:: python
   pytest_mproc.Settings.set_ssh_credentials(username, optional_password)


Directories for caching  (e.g., where any test requirements are installed on a per-project-name basis peristently)
as well as the temp directory where bundles are deployed can also be specified, can be configured:

.. code-block:: python
   fromt pathlib import Path
   pytest_mproc.Settings.set_cached_dir(Path("/common/path/to/cache_dir/on/remote/nodes"))
   pytest_mproc.Settings.set_tmp_root(Path("/common/path/to/cache_dir/on/remote/nodes"))

These will be common across all nodes, and thus all nodes must support the directory configuration.

Remote Hosts Specification
--------------------------

The remote hosts configuration (which nodes the tests execute) is specified on the command line through
the *--remote-worker* command line option. The value of this option can take several forms:

* a fixed host specification in the form of "<host>[:<port>];optoin1=value1;...";  the option/value pair can be
  repeated to specify multiple remote clients
* a file with line-by-line JSON content specifying the remote host/port and options as specified below; the option
  cannot be repeated
* an http end point that returns the remote host/port and options, again in line-by-line format.  The option can
  be repeated to specify backup end points as necessary

Use of a server for managin a cloud of resources is discussed in more detail below.

The format of the http response or in the file is in a line-by-line JSON format.  For example, if I am using pytest
to drive execution from remote hosts against attached Android devices, the content might look like:

.. code-block:: json

   {"host": "host.name.or.ip", "arguments": {"device": "ANDROID_DEVICE1", "argument1": "value1", "ENV_VAR": "value1"}}

Arguments that are all upper case names will be passed to the remote host as environment variables.  All others will
be passed to pytest as command line options in the form "--<option> <value>" and your pytest must be configured
to add and accept those command line options.  There are a few command line options that are reserved for use
by *pytest_mproc*:
* *cores* : specifies the number of cores (worker threads) to instantiate on the remote host;  if not specified
the number of cores specified on the main host command line will be used (see below)
* *jump_host* : if specified, this host:port pair will be used to add a jump host option when invoking ssh against
the remote host

Interface *pytest_mproc* with a Cloud
.....................................

Hard-coding worker node ip addresses in a file or on a command line is not ideal.  Although it provides a
brut-force method for ensuring viability of distributed testing without much setup, a more dynamic alternative
is needed in most cases.  *pytest_mproc* provides an interface to interact with an externally developed
http (micro) service as proxy for access into a cloud.  Reservation of resources (and relinquishing those
resources when testing ends) is achieved by exercising this REST-ful interface.



When using an http endpoint, *pytest_mproc* will do lazy loading of the response.  This is to cover the case where
the request to the endpoint is based on a reservation system that reserves specific resources.  (One example is
using pytest to drive device testing of a mobile or smart device.)  The resource may require
preparation time before returning a resrouce back to the main host, or may only be able to
return a subset of the requested resources until others become available.  In this way, *pytest_mproc* can
start execution against the available resources while waiting for the others to become available (optimized
execution strategy).

If using a web backend, the first line of json should contain the following (with no return characters of course):

.. code-block:: json
   {"session_id": "unique_sessio_id", "end_session_url" : "https://url/to/end/session",
    "start_session_url": "https://url/to/start/session", "heartbeat_url": "https://url/to/provide/a/heartbeat"}

The two optional URLs assume that the reservatio system is session-based and provides a way for *pytest_mproc* to end
the session, as well as signal that the session is still active. The *heartbeat_url"  is a way for *pytest_mproc" to
provide a heartbeat to the session/reservation system that the session is still active (to be provided at at least
a 1Hz rate).  The *end_url* is a way for *pytest_mproc* to signal end of session (allowing the reservation system
to relinquish any resources back to the system).  Both of these will be invoked as simplet GET requests.


Running the Main CLI
....................

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
