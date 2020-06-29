#!/bin/bash
echo "Running parallel stress tests..."
PYTHONPATH=`pwd`/../src:`pwd`/.. timeout 60 pytest --cov=testcode --cov-report term --cov-report html -s --cores auto*10 test_mproc_runs.py -k alg2  && export error=1
if test -n "$error"; then
   echo -e "\e[32mPASSED: \e[39m PASSED: got expected successful return status"
else
   echo -e "\e[31mFAIL: \e[39m Did not get expected successful return status from test failure"
   exit $error;
fi;



