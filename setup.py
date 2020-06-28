import setuptools

VERSION = "3.2.13"

setuptools.setup(
    name='pytest_mproc',
    author='John Rusnak',
    author_email='jrusnak@linkedin.com',
    version=VERSION,
    description="low-startup-overhead, scalable, distributed-testing pytest plugin",
    package_dir={'': 'src'},
    packages=setuptools.find_packages('src'),
    entry_points={
       "pytest11": ["name_of_plugin = pytest_mproc.plugin"],
    },
    classifiers=["Framework :: Pytest",
                 "Development Status :: 4 - Beta",
                 "License :: OSI Approved :: BSD License"],
    license='BSD 2-CLAUSE',
    keywrds='pytest distributed multiprocessing',
    url='https://github.com/jrusnakli/pytest_mpdist',
    download_url="https://github.com/jrusnakli/pytest_mpdist/dist/%s" % VERSION,
    install_requires=[
        'pytest',
        'pytest-cov'
    ]
)
