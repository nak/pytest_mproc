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

You can also specify things like "auto*2" or "auto/2" for number of cores

Disabling Mproc from Command Line
=================================

To disable mproc (overriding all other pytest-mproc-related flags), specify "--diable-mproc True" on the command line.

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

