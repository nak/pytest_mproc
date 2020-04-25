#!/bin/bash
echo "Running parallel tests..."
PYTHONPATH=`pwd`/../src:`pwd`/.. pytest --cov=testcode --cov-report term --cov-report html -s --cores 4 test_mproc_runs.py >& output.txt && export error=1
if test -n "$error"; then
   echo -e "\e[31mFAIL: \e[39m Did not get expected error status from test failure"
   exit $error;
else
   echo -e "\e[32mPASSED: \e[39m PASSED: got expected error return status from test failure"
fi;
if test -n "`grep \"== 1 failed, 1004 passed\" output.txt`"; then
 echo  -e "\e[32mPASSED: \e[39m Pass/fail counts as expected"
else
 echo -e "\e[31mFAILED: \e[39mPass/fail counts not as expected"
 cat output.txt
 exit 2
fi;

echo "Running serially..."
unset error
PYTHONPATH=`pwd`/../src:`pwd`/.. pytest -s test_mproc_runs.py >& output2.txt && export error=2
if test -n "$error"; then
   echo -e "\e[31mFAIL: \e[39m Did not produce expected error status from test failure"
   exit $error;
else
   echo -e "\e[32mPASSED: \e[39m Produced expected error return status from test failure"
fi;
if test -n "`grep \"== 1 failed, 1004 passed\" output.txt`"; then
 echo  -e "\e[32mPASSED: \e[39m Pass/fail counts as expected"
else
 echo -e "\e[31mFAILED: \e[39mPass/fail counts not as expected"
 cat output2.txt
 exit 3
fi;

echo "Running off-nominal cases..."
echo "->  Exception in fixture"
OUTPUT_EXC=output_fixture_exception.txt
PYTHONPATH=`pwd`/../src:`pwd`/.. pytest -s ../testcode/error_cases/fixture_errors.py >& $OUTPUT_EXC
if test -n "`grep INTERNAL $OUTPUT_EXC`"; then
  echo -e "\e[31mFAILED: \e[39mFound INTERNAL error markers in user-based exception in fixture"
  cat $OUTPUT_EXC
  exit 3;
else
    echo -e "\e[32mPASSED: \e[39m No internal errors persent in user-side exception"
fi;

if test -z "`grep Traceback output_fixture_exception.txt`"; then
   echo -e "\e[31mFAILED: \e[39m Failed to produce traceback from user-exception in global test fixture"
   cat $OUTPUT_EXC
   exit 7
else
    echo -e "\e[32mPASSED: \e[39m User-based traceback present as expected in user-based test fixture exception"
fi;

echo "-> Improper global fixture depdnency"
PYTHONPATH=`pwd`/../src:`pwd`/.. pytest -s ../testcode/error_cases/fixture_wrong_dependency.py >& output_dep.txt || export error_dep=10

if test -z "`grep \"ScopeMismatch: You tried to access the 'session' scoped fixture 'fixture_session' with a 'global' scoped request object, involved factories\" output_dep.txt`"; then
   echo -e "\e[31mFAILED: \e[39m Failed to report improper dependency of global test fixture on lesser (session) fixture"
   exit 8
else
   echo -e "\e[32mPASSED: \e[39m Reported improper dependency of global test fixture on lesser (session) fixture"
fi;

echo -e "All tests \e[32mPASSED: \e[39m"
