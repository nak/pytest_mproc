from typing import Optional


def pytest_addoption(parser):
    group = parser.getgroup("pytest_mproc", "better distributed testing through multiprocessing")
    group._addoption(
            '--loop',
            dest='loops',
            action='store',
            type='int',
            help='number of times to iterate each test',
        )


loops: Optional[int] = None


def pytest_cmdline_main(config):
    global loops
    loops = config.getoption('--loop', default=None)


def pytest_generate_tests(metafunc):
    if loops:
        metafunc.fixturenames.append('loop_count')
        metafunc.parametrize('loop_count', [i for i in range(loops)])

