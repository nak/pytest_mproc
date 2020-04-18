"""
This package contains code to coordinate execution from a main thread to worker threads (processes)
"""

import select
import sys
import time

from _pytest.config import _prepareconfig
from multiprocessing import Process, Queue
from pytest_mproc.worker import main as worker_main


class Coordinator:
    """
    Context manager for kicking of worker Processes to conduct test execution via pytest hooks
    """

    def __init__(self, num_processes):
        """
        :param num_processes: number of parallel executions to be conducted
        """
        self._num_processes = num_processes
        self._tests = []  # set later

        # test_q is for sending test nodeid's to worked
        # result_q is for receiving results as messages, exceptions, test status or any exceptions thrown
        self._result_qs = [Queue() for _ in range(num_processes)]
        self._test_q = Queue()
        self._processes = []
        self._count = 0
        self._session_start_time = time.time()
        self._process_status_text = ["" for _ in range(num_processes)]

    def start(self):
        """
        Start all worker processes

        :return: this object
        """
        for index in range(self._num_processes):
            proc = Process(target=worker_main, args=(index, self._test_q, self._result_qs[index], self._num_processes))
            self._processes.append(proc)
            proc.start()
        return self

    def set_items(self, tests):
        """
        :param tests: the items containing the pytest hooks to the tests to be run
        """
        grouped = [t for t in tests if getattr(t._pyfuncitem.obj, "_pytest_group", None)]
        self._tests = [t for t in tests if t not in grouped]
        groups = {}
        for g in grouped:
            groups.setdefault(g._pyfuncitem.obj._pytest_group, []).append(g)
        for test_list in groups.values():
            self._tests.insert(0, test_list)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()

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

    def _output_summary(self):
        """
        Output the summary of test execution
        """
        self._write_sep('=', "STATS")
        for msg in self._process_status_text:
            sys.stdout.write(msg)
        length = sum([1 if not isinstance(t, list) else len(t) for t in self._tests])
        if self._count != length:
            self._write_sep('!', "{} tests unaccounted for {} out of {}".format(length - self._count,
                            self._count, length))
        sys.stdout.flush()

    def _process_worker_message(self, session, typ, data):
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
                index, worker_count, exitstatus, duration = data  # which process and count of tests run
                name = "worker-%d" % (index + 1)
                self._process_status_text[index] = "Process %s executed %d tests in %.2f seconds\n" % (
                    name, worker_count, duration)
                try:
                    self._processes[index].join(5)
                except Exception as e:
                    pass
                self._processes[index].terminate()
                self._processes[index] = None
                self._result_qs[index].close()
                self._result_qs[index] = None
            elif typ == 'error_message':
                error_msg_text = data
                sys.stdout.write("{}\n".format(error_msg_text))
            elif typ == 'exception':
                # reraise any exception from workers
                raise Exception("Exception in worker process: %s" % str(data))

        except Exception as e:
            sys.stdout.write("INTERNAL_ERROR> %s\n" % str(e))

    def run(self, session):
        """
        Populate test queue and continue to process messages from worker Processes until they complete

        :param session: Pytest test session, to get session or config information
        """
        reader_mapping = {q._reader: q for q in self._result_qs}

        def read_results(timeout=0):
            available_qs = [q._reader for q in self._result_qs if q is not None]
            (inputs, _, _) = select.select(available_qs, [], [], timeout)
            while inputs:
                for input in inputs:
                    items = reader_mapping[input].get()
                    for kind, data in items:
                        self._process_worker_message(session, kind, data)
                available_qs = [q._reader for q in self._result_qs if q is not None]
                (inputs, _, _) = select.select(available_qs, [], [], timeout)

        #while any(self._result_qs):
        for test in self._tests:
            if all([q is None for q in self._result_qs]):
                raise Exception("Tests remain but no workers left to process them!")
            read_results()
            # Function objects in pytest are not pickle-able, so have to send string nodeid and
            # do lookup on worker side
            item = [t.nodeid for t in test] if isinstance(test, list) else [test.nodeid]
            self._test_q.put(item)

        read_results()
        for _ in self._processes:
            self._test_q.put(None)  # signals end

        while any([q is not None for q in self._result_qs]):
            read_results(timeout=1)
        self._test_q.close()
        sys.stdout.write("\r\n")
        self._output_summary()


if __name__ == "__main__":
    args = sys.argv[1:]
    plugins_to_load = []
    config = _prepareconfig(args, plugins_to_load)
    config.hook.pytest_cmdline_main(config=config)
