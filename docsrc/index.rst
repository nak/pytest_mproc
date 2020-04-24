.. pytest_mproc documentation master file, created by
   sphinx-quickstart on Sat Feb  8 16:47:29 2020.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

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

Global Initializers/Finalizers
==============================

.. warning::
    Much like pytest-xdist, session level variables are called once for EACH THREAD.  So if you run on 4 cores, each
    session-scoped fixture will run 4 times.  This is because forking is used to parallelize tests.

To add an initializer and finalzier to be called only once before all tests execute and after all tests executed,
respectively, use fixture from the pytest_mproc_utils package:

.. code-block:: python

   from pytest_mproc.utils import global_initializer, global_finalizer
   @pytest_mproc_global_initializer
   def initualizer():
       pass

   @pytest_mproc_global_finalizer
   def finalizer():
       pass

Global Session Context Manager
==============================

Alternatively, you can use a context manager (either a function with a single yield or a class with __enter__ and __exit__ methods). The function,
or the __init__ method in the case of a class, must not take any arguments.

.. code-block:: python

   from pytest_mproc.utils import global_session_context

   @global_session_context
   class ContextManager:

       def __enter__(self):
           # setup some resource globally once for all tests/processes being run
           return self

       def __exit__(self):
           # teardown/shutdown resources set up on enter
           pass




