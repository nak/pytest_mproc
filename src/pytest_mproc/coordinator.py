"""
This package contains code to coordinate execution from a main thread to worker threads (processes)
"""
import resource
import sys
import time
from typing import List, Any

from _pytest.config import _prepareconfig
from multiprocessing import Process, Queue

from pytest_mproc import resource_utilization
from pytest_mproc.worker import main as worker_main


class Coordinator:
    """
    Context manager for kicking of worker Processes to conduct test execution via pytest hooks
    """

    def __init__(self, num_processes, is_master: bool):
        """
        :param num_processes: number of parallel executions to be conducted
        """
        self._num_processes = num_processes
        self._tests = []  # set later
        self._is_master = is_master
        # test_q is for sending test nodeid's to worked
        # result_q is for receiving results as messages, exceptions, test status or any exceptions thrown
        self._processes = []
        self._count = 0
        self._rusage = []
        self._session_start_time = time.time()
        self._test_q: Queue = None  # set on call to start()
        self._result_q: Queue = None  # set on call to start()

    @classmethod
    def do_work(cls, config, num_processes: int, test_q: Queue, result_q: Queue):
        processes : List[Process] = []
        for index in range(num_processes):
            from .plugin import Node
            proc = Process(target=worker_main, args=(index, test_q, result_q, num_processes, Node.Manager.PORT))
            processes.append(proc)
            proc.start()
        for proc in processes:
            proc.join()
        from .plugin import Global
        client = Global.Manager(config, False)
        client.signal_satellite_done(num_processes)

    def populate_test_queue(self, tests: List[Any], config):
        for test in tests:
            # Function objects in pytest are not pickle-able, so have to send string nodeid and
            # do lookup on worker side
            item = [t.nodeid for t in test] if isinstance(test, list) else [test.nodeid]
            self._test_q.put(item)
            # signal all workers test q is exhausted:
        from .plugin import Global
        client = Global.Manager(config, False)
        client.signal_test_q_exhausted()
        #for _ in range(self._num_processes):
        #    self._test_q.put(None)

    def start(self, config):
        """
        Start all worker processes

        :return: this object
        """
        from .plugin import Global
        client = Global.Manager(config, False)
        client.register_satellite(self._num_processes)
        self._test_q: Queue = client.get_test_queue()
        self._result_q: Queue = client.get_results_queue()
        self._worker_manager_process = Process(target=self.do_work,
                                               args=(config, self._num_processes, self._test_q, self._result_q))
        self._worker_manager_process.start()
        return self

    def set_items(self, tests):
        """
        :param tests: the items containing the pytest hooks to the tests to be run
        """
        grouped = [t for t in tests if getattr(t._pyfuncitem.obj, "_pytest_group", None)]
        self._tests = [t for t in tests if t not in grouped]
        groups = {}
        for g in grouped:
            name, priority = g._pyfuncitem.obj._pytest_group
            groups.setdefault((name, priority), []).append(g)
        for key in sorted(groups.keys(), key=lambda x: x[1]):
            self._tests.insert(0, groups[key])

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()

    def kill(self):
        for proc in self._processes:
            if proc:
                proc.terminate()

    def stop(self, delay=None):
        # shouldn't be any procs left, but just in case
        for proc in self._processes:
            if proc:
                try:
                    if delay is not None:
                        proc.join(delay)
                except Exception:
                    pass
                try:
                    proc.terminate()
                except Exception:
                    pass

    @staticmethod
    def _write_sep(s, txt):
        """
        write out text to stdout surrounded by repeated character

        :param s: character to repeat on either side of given text
        :param txt: text to by surrounded
        """
        sep_total = max((70 - 2 - len(txt)), 2)
        sep_len = sep_total // 2
        sep_extra = sep_total % 2
        out = '%s %s %s\n' % (s * sep_len, txt, s * (sep_len + sep_extra))
        sys.stdout.write(out)

    def _output_summary(self, time_span: float, ucpu: float, scpu: float, unshared_mem: float):
        """
        Output the summary of test execution
        """
        self._write_sep('=', "STATS")
        sys.stdout.write("User CPU, System CPU utilization, Add'l memory during run\n")
        sys.stdout.write("---------------------------------------------------------\n")
        for index, (count, duration, ucpu, scpu, mem) in enumerate(self._rusage):
            sys.stdout.write(f"Process Worker-{index} executed {count} tests in {duration:.2f} " +\
                             f"seconds; User CPU: {ucpu:.2f}%, Sys CPU: {scpu:.2f}%, " +\
                             f"Mem consumed: {unshared_mem/1000.0}M\n")
        sys.stdout.write("\n")
        sys.stdout.write(
            f"Process Coordinator executed in {time_span:.2f} seconds. " +
            f"User CPU: {ucpu:.2f}%, Sys CPU: {scpu:.2f}%, " +
            f"Mem consumed: {unshared_mem/1000.0}M\n"
        )
        length = sum([1 if not isinstance(t, list) else len(t) for t in self._tests])
        if self._count != length:
            self._write_sep('!', "{} tests unaccounted for {} out of {}".format(length - self._count,
                            self._count, length))
        sys.stdout.flush()

    def _process_worker_message(self, session, typ, data, ):
        """
        Process a message (as a worker) from the coordinating process

        :param session: the pytest test session
        :param typ: the kind of messsage
        :param data: payload for the message to be processed
        """
        try:
            # TODO: possibly have more structured communications, but for now
            # there are limited types of messages, so sendo message, <data> tuple
            # seems to be fine?
            if typ == 'test_status':
                report = data
                if report.when == 'call' or (report.when == 'setup' and not report.passed):
                    self._count += 1
                session.config.hook.pytest_runtest_logreport(report=report)
                if report.failed:
                    sys.stdout.write("\n%s FAILED\n" % report.nodeid)
            elif typ == 'exit':
                # process is complete, so close it and set to None
                index, worker_count, exitstatus, duration, rusage = data  # which process and count of tests run
                time_span, ucpu, scpu, unshared_mem = rusage
                self._rusage.append((worker_count, time_span, ucpu, scpu, unshared_mem))
            elif typ == 'error_message':
                error_msg_text = data
                sys.stdout.write("{}\n".format(error_msg_text))
            elif typ == 'exception':
                # reraise any exception from workers
                raise Exception("Exception in worker process: %s" % str(data))

        except Exception as e:
            sys.stdout.write("INTERNAL_ERROR> %s\n" % str(e))

    def read_results(self, session):
        items = self._result_q.get()
        while items is not None:
            for kind, data in items:
                self._process_worker_message(session, kind, data)
            items = self._result_q.get()

    def run(self, session):
        """
        Populate test queue and continue to process messages from worker Processes until they complete

        :param session: Pytest test session, to get session or config information
        """
        start_rusage = resource.getrusage(resource.RUSAGE_SELF)
        start_time = time.time()

        # we are the root node, so populate the tests
        if self._is_master:
            populate_tests_process = Process(target=self.populate_test_queue, args=(self._tests, session.config))
            populate_tests_process.start()
            self.read_results(session)  # only master will read results and post reports through pytest
            populate_tests_process.join(timeout=1)  # should never time out since workers are done
        self._worker_manager_process.join()
        if self._is_master:
            self._test_q.close()
            self._result_q.close()
        end_rusage = resource.getrusage(resource.RUSAGE_SELF)
        time_span = time.time() - start_time
        time_span, ucpu, scpu, addl_mem_usg = resource_utilization(time_span=time_span,
                                                        start_rusage=start_rusage,
                                                        end_rusage=end_rusage)

        sys.stdout.write("\r\n")
        self._output_summary(time_span, ucpu, scpu, addl_mem_usg)


if __name__ == "__main__":
    args = sys.argv[1:]
    plugins_to_load = []
    config = _prepareconfig(args, plugins_to_load)
    config.hook.pytest_cmdline_main(config=config)
