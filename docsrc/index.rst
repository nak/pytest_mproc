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

Welcome to pytest-mproc, a plugin for pytest to run distributed testing via multiprocessing.  This manner
of distributed testing has several advantages, including more efficient execution over pytest-xdist in many cases.

xdist works for cases perhaps where there are large number of tests, but the cost of overhead in using rsync and
overall implementation prevents realization of test-execution-time gains that one might hope for.

To begin using pytest_mproc, just install:

% pip install pytest-mproc

Currently, *pytest_mproc* is only usable in the context of a single host with multiple cores, although future
support may be added for distribute testing across multiple nodes.

Why Use *pytest_mproc* over *pytest-xdist*?
===========================================

*pytest_mproc* has several advantages over xdist, depending on your situation:

#. Overhead of startup is more efficient, and start-up time does not grow with increasing number of cores
#. It uses a pull model, so that each worker pulls the next test from a master queue.  There is no need to figure
out how to divy up the tests beforehand.  The disadvantage is that the developer needs to know how long roughly each
test takes and prioritize the longer running tests first (*@pytest.mark.tryfirst*) .
#. It provides a 'global' scope test fixture to provide a single instance of a fixture across all tests, regardless of
how many nodes
#. It allows you to programatically group together a bunch of tests to run serially on a single worker process (and
run these first)


Usage
=====

To use with pytest, you use the -C or equivalently --cores argument to specify how many cores to run on:

% pytest --cores 3 [remaining arguments]

or to specify use of all available cores:

% pytest --cores auto [remaining arguments]

You can also specify things like "auto*2" or "auto/2" for number of cores.

.. caution::
   Overscribing CPUs will cause additional overhead.  *pytest_mproc* kicks off all worker threads up front, and
   each worker does identical processing to independently come up with the list of tests.  When the number of
   threads is less than or equal to the number of cores ("a batch"), this adds little overhead.  Once beyond that, each
   additional (partial) batch will add more overhead, as the CPU must be split across multiple threads for some or all
   cores.

Disabling Mproc from Command Line
=================================

To disable mproc (overriding all other pytest-mproc-related flags), specify "--diable-mproc True" on the command line.

Command Line Configuration Options
==================================

Aside from specifying the number of cores, the user can optionally specify the maximum number of active connections
allowed during distributed processing.  The *multiprocessing* module can get bogged down and dwell on one thread or
even deadlock when too many connection requests are made at one time to the main process.  To alleviate this,
the number of allowed simultaneous connections is throttled, with a default of 24 maximum connections.  The user can
over ride this using the *"--max-simultanous-connections" option.


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

   TEST_GROUP_CLASSS = "class_group"

   @pytest.mproc.group(TEST_GROUP_CLASS)
   class TestClass:
       def test_method1(self):
          pass

       def test_method2(self):
          pass

This is useful if the tests are using a common resource for testing and parallelized execution of tests might
result in interference.

.. note:: Grouped tests are always placed at the front of the test queue  -- i.e, always scheduled to run before
ungrouped tests

A Matter of Priority
--------------------
You can also specify a priority of scheduling for groups.  The group annotation takes an optional priority as
the second argument:

.. code-block:: python

   import pytest_mproc

   @pytest.mpro.group("Prioritzed_Group", priority=1)
   def test_function():
      pass

Groups with lower interger priority value are scheduled before those with a higher priority value.  Thus, a
group with priority *0* will be scheduled before a group with priority *1*.  The default priority is *0* if
not specified.

A 'global'-ly Scoped Fixture
============================

As is the case with *pytest_xdist* plugin, *pytest_mproc* uses multiprocessing module to achieve parallel concurrency.  This
raises interesting behaviors about 'session' scoped fixtures.  Each subprocess that is launched will create its own
session-level fixture;  in other words, you can think of a session as a per-process concept, with one session-level
fixture per process.  This is often not obvious to test developers. Many times, a global fixture is needed, with a
single instantiation across all processes and tests.  An example might be setting up a single test database.

To achieve this, *pytest_mproc* adds a new scope: 'global'. Behind the scenes, the system uses pythons *multiprocessing*
module's BaseManager to provide this feature.  This is mostly transparent to the developer, with one notable exception.
In order to communicate a fixture globally across multiple *Process*es, the object returned (or yielded) from a
globally scoped fixture must be Picklable.

The 'global' scope is a level above 'session' in the hierarchy.  That means that globally scoped fixtures can only
depend on other globally scoped fixtures.

.. warning::
    Values returned or yielded from a fixture with scope'global' must be picklable so as to be shared across multiple
    independent subprocesses.


.. code-block:: python

   import pytest

   @pytest.fixture(scope='global')
   def globally_scoped_fixture()
       ...


Tips for Using Global Scope Fixture
-----------------------------------
Admittedly, one has to think about how multiprocessing works to know what can and cannot be returned from a global
test fixture.  Since they are singltons, one useful tip if you are using (singleton) classes (such as manager classses) to
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

A Safe *tmpdir* Fixture
=======================

*pytest_mproc* provides its own fixtures for creating temporary directories: *mp_tmpdir_factory*, a fixture with a
"*create_tmpdir*" method for creating multiple temporary directories within a test procedure, and a
*mp_tmpdir* directory for creating a single temporary directory.  Other plugins providing similar featurs can have
race conditions causing intermittent (if infrequent) failures.  This plugin guarantees to be free of race conditions
and safe in the context of *pytest_mproc*'s concrurrent test framework.

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
use the "--as-server" command line argument, specifying the port the server will listen on:

.. code-block:: shell

   % pytest --num_cores <N> --as-server <port> ...

When *--as-server* argument is presnt, N may be zero, indicating
the process will wait indefinitely for workers on other nodes to be created to do the testing.  In
this case, not tests will be executed on the main node.  If N > 0, then test execution will start immediately
on the main node following test setup;  as workers on other nodes come on line, they will pull tests from those
remaining the queue and execute in parallel to the main.

To start the client nodes:

.. code-block:: shell

   % pytest --num_cores <N> --as-client <host>:<port>

Here, N must be greater than or equal to 1;  multiple workers can be invoked on a node, with possibly multiple client
nodes. The client will attempt to connect to the main  node for a period of 30 seconds at which point it gives up and
exits with na error.

.. note::
   *pytest_mproc* does not have the facilities to launch testing on other external nodes.  The checkout of code on
   client machines and execution of the pytest command on client nodes falls outside of *pytest_mproc*'s scope


Node Scoped Fixtures
--------------------

With the system now allowing execution of mutliple workers on a single machine, and execution across multiple machines,
this introduces a new level of scoping of fixtures:  *node*-scoped fixtures.  A fixture scoped to '*node*' is
instantiated once per node and such instance is only available to workers on that node.  The '*global*'-scoped fixtures
still hold the meaning of being scoped to the entirety of testing, one instance available to all workers across all
machines (so thinkg carefully about what you are providing in your *global* fixture!).

To declare a fixture to be scoped to a node:

.. code-block:: python

   @pytest.fixture(scope='node')
   def node_scoped_fixture_function():
       ...


