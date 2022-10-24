from pathlib import Path

import setuptools

VERSION = "5.1.18a"

requirements = []
with open(Path(__file__).parent / 'requirements.txt', 'r') as in_stream:
    for line in in_stream:
        line = line.strip()
        if line:
            requirements.append(line)

setuptools.setup(
    name='pytest_mproc',
    author='John Rusnak',
    author_email='jrusnak69@gmail.com',
    version=VERSION,
    description="low-startup-overhead, scalable, distributed-testing pytest plugin",
    package_dir={'': 'src'},
    package_data={'': ['pure_requirements.txt', 'impure_requirements.txt']},
    include_package_data=True,
    packages=setuptools.find_packages('src'),
    entry_points={
       "pytest11": ["name_of_plugin = pytest_mproc.plugin"],
    },
    classifiers=["Framework :: Pytest",
                 "Development Status :: 4 - Beta",
                 "License :: OSI Approved :: BSD License"],
    license='BSD 2-CLAUSE',
    keywords='pytest distributed multiprocessing',
    url='https://github.com/nak/pytest_mproc',
    download_url="https://github.com/nak/pytest_mproc/dist/%s" % VERSION,
    install_requires=requirements
)
