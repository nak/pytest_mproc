"""
This package contains code to coordinate execution from a main thread to worker threads (processes)
"""
import os
import resource
import signal
import sys
import time
from typing import List, Any

from _pytest.config import _prepareconfig
from multiprocessing import Process, Queue, Semaphore

from pytest_mproc import resource_utilization
from pytest_mproc.config import MPManagerConfig, RoleEnum
from pytest_mproc.utils import BasicReporter
from pytest_mproc.worker import main as worker_main
from pytest_mproc.fixtures import Global


class Coordinator:
    """
    Context manager for kicking of worker Processes to conduct test execution via pytest hooks
    """

    def __init__(self, config: MPManagerConfig):
        """
        :param num_processes: number of parallel executions to be conducted
        """
        self._num_processes = config.num_processes
        self._tests = []  # set later
        self._role = config.role
        # test_q is for sending test nodeid's to worked
        # result_q is for receiving results as messages, exceptions, test status or any exceptions thrown
        self._count = 0
        self._rusage = []
        self._session_start_time = time.time()
        self._test_q: Queue = None  # set on call to start()
        self._result_q: Queue = None  # set on call to start()
        self._reporter = BasicReporter()

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
        if self._count != length and self._role == RoleEnum.MASTER:
            self._write_sep('!', "{} tests unaccounted for {} out of {}".format(length - self._count,
                            self._count, length))
        sys.stdout.flush()

    def _process_worker_message(self, hook, typ, data, ):
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
                hook.pytest_runtest_logreport(report=report)
                if report.failed:
                    sys.stdout.write("\n%s FAILED\n" % report.nodeid)
            elif typ == 'exit':
                # process is complete, so close it and set to None
                try:
                    index, worker_count, exitstatus, duration, rusage = data  # which process and count of tests run
                    time_span, ucpu, scpu, unshared_mem = rusage
                    self._rusage.append((worker_count, time_span, ucpu, scpu, unshared_mem))
                except:
                    pass
                    # self._reporter.write(f"Unable to process rusage: {data}\n")
            elif typ == 'error_message':
                error_msg_text = data
                sys.stdout.write("{}\n".format(error_msg_text))
            elif typ == 'exception':
                # reraise any exception from workers
                raise Exception("Exception in worker process: %s" % str(data)) from data

        except Exception as e:
            import traceback
            traceback.print_exc()
            sys.stdout.write("INTERNAL_ERROR> %s\n" % str(e))

    def read_results(self, hook):
        items = self._result_q.get()
        exited = []
        while items is not None:
            if isinstance(items, Exception):
                raise Exception
            for kind, data in items:
                self._process_worker_message(hook, kind, data)
            items = self._result_q.get()
            if items:
                exited += [item[1][0] for item in items if item[0] == 'exit']
                if len(exited) == self._num_processes:
                    break

    def do_work(self, mpconfig: MPManagerConfig, start_sem: Semaphore):
        processes : List[Process] = []
        try:
            connect_sem: List[Semaphore] = [Semaphore(0) for i in range(mpconfig.num_processes)]
            for index in range(mpconfig.num_processes):
                args = mpconfig.__dict__
                args.update({'role': RoleEnum.WORKER})
                worker_config = MPManagerConfig(**args)
                proc = Process(target=worker_main, args=(index, worker_config, mpconfig.role != RoleEnum.MASTER,
                                                         connect_sem[index], self._reporter))
                processes.append(proc)
                proc.start()

            def kill_all(*args, **kargs):
                self._reporter.write("Termination signal received;  killing all worekrs...")
                for _ in range(self._num_processes):
                    self._test_q.put(None, timeout=0)
                for proc in processes:
                    proc.terminate()
                    proc.join(timeout=2)
                    processes.remove(proc)

            signal.signal(signal.SIGTERM, kill_all)
            start_sem.release()
            client = Global.Manager(mpconfig, force_as_client=True)
            client.wait_lock().acquire()
            client.wait_lock().release()
            for index, sem in enumerate(connect_sem):
                if sem.acquire(timeout=0) is not True:
                    self._reporter.write(f"Worker-{index} failed to connect in time.  Aborting thread\n", red=True)
                    os.kill(processes[index].pid, signal.SIGTERM)
                    processes[index].join()
                    self._result_q.put([('exit', (index, -1, -1, -1, (-1. -1, -1, -1)))])
                    processes[index] = None
            for index, proc in enumerate(processes):
                if proc:
                    proc.join()
            client.signal_satellite_done(mpconfig.num_processes)
        except Exception as e:
            import traceback
            self._reporter.write(f">>>>>>>>>>> ERROR in worker manager process: str{e} \n{traceback.format_exc()}\n")
            for index, proc in enumerate(processes):
                if proc:
                    os.kill(processes[index].pid, signal.SIGKILL)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            self._worker_manager_process.join(timeout=2)
        except Exception:
            self.kill()

    def populate_test_queue(self, tests: List[Any], mpconfig: MPManagerConfig):
        for test in tests:
            # Function objects in pytest are not pickle-able, so have to send string nodeid and
            # do lookup on worker side
            item = [t.nodeid for t in test] if isinstance(test, list) else [test.nodeid]
            self._test_q.put(item)
            # signal all workers test q is exhausted:
        client = Global.Manager(mpconfig, force_as_client=True)
        client.signal_test_q_populated()

    def start(self, mpconfig: MPManagerConfig):
        """
        Start all worker processes

        :return: this object
        """
        client = Global.Manager(mpconfig, force_as_client=True)
        client.register_satellite(mpconfig.num_processes)
        self._test_q: Queue = client.get_test_queue()
        self._result_q: Queue = client.get_results_queue()
        start_sem = Semaphore(0)
        self._worker_manager_process = Process(target=self.do_work, args=(mpconfig, start_sem))
        self._worker_manager_process.start()
        start_sem.acquire()
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

    def kill(self):
        self._worker_manager_process.terminate()
        self._worker_manager_process.join(timeout=5)

    def run(self, session):
        """
        Populate test queue and continue to process messages from worker Processes until they complete

        :param session: Pytest test session, to get session or config information
        """
        start_rusage = resource.getrusage(resource.RUSAGE_SELF)
        start_time = time.time()
        mpconfig = session.config.option.mpconfig
        # we are the root node, so populate the tests
        if mpconfig.role == RoleEnum.MASTER:
            populate_tests_process = Process(target=self.populate_test_queue, args=(self._tests, mpconfig))
            populate_tests_process.start()
            self.read_results(session.config.hook)  # only master will read results and post reports through pytest
            populate_tests_process.join(timeout=1)  # should never time out since workers are done
        self._worker_manager_process.join()
        if mpconfig.role == RoleEnum.MASTER:
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
