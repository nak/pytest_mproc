***************
Developer Guide
***************

The packages for this pytest plugin are divided into three main modules:

#. *coordinator* - for coordinating parallel execution from a coordinator process to multiple worker processes
#. *worker* - contains code for processing tests as they are popped off of the queue (a queue fed by the coordinator)
#. *plugin* - contains the pytest standard hooks for processing pytest arguments and performing actions at key
   points along the test execution flow

The system starts by starting all threads (processes).  Each thread processes the list of tests available using
standard pytest code.  This may seem inefficient in terms of CPU usage, but since processes are each on their own
core, very little overall time is wasted.  The reason for doing this is that pytest items are not serializable to
send across a queue, and so only test nodeid's are sent for lookup by the worker node.

The system functions by having the coordinator node push all tests to a queue shared with all workers.  The workers then
grab the next available test from the queue for execution.  This pull model alleviates any additional work needed by
the developer to figure out how to split up their tests for execution.  Ideally, longer running tests would be executed
first and short tests run last, to best distribute the workload.

A Note about Running Tests for pytest_mproc
============================================

Please note that one of the tests for this product is designed to fail deliberately.  This is to test that test
status is reported properly for failed tests when using this pytest plugin.

Coordinator
===========

.. automodule:: pytest_mproc.coordinator

.. autoclass:: pytest_mproc.coordinator.Coordinator
   :members:

Worker
======

.. automodule:: pytest_mproc.worker

.. autoclass:: pytest_mproc.worker.WorkerSession
   :members:

