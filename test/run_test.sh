#!/bin/bash
echo "Running parallel tests..."
PYTHONPATH=`pwd`/../src:`pwd`/.. pytest --cov=testcode --cov-report term --cov-report html -s --cores 4 test_mproc_runs.py >& output.txt && echo -e "\e[31mFAIL: \e[39mExpected error status" && exit 1 || echo -e "\e[32mPASS:\e[39m Got expected error return code"
if test -n "`grep \"== 1 failed, 1004 passed\" output.txt`"; then
 echo  -e "\e[32mPASSED: \e[39m Pass/fail counts as expected"
else
 echo -e "\e[31mFAILED: \e[39mPass/fail counts not as expected"
 exit 1
fi;

echo "Running serially..."
PYTHONPATH=`pwd`/../src:`pwd`/.. pytest -s test_mproc_runs.py >& output2.txt && echo -e "\e[31mFAIL: \e[39mExpected error status" && exit 5 || echo -e "\e[32mPASS:\e[39m Got expected error return code"
if test -n "`grep \"== 1 failed, 1004 passed\" output.txt`"; then
 echo  -e "\e[32mPASSED: \e[39m Pass/fail counts as expected"
else
 echo -e "\e[31mFAILED: \e[39mPass/fail counts not as expected"
 exit 1
fi;
