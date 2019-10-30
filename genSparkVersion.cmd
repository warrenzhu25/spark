rem Set the command to generate full hash of current commit
rem Generate SPARK_VERSION as the first 8 chars of full hash
rem
rem This script repeats part some of code from YarnForAP\Hadoop\genHadoopVersion.cmd

set cmd="git log -n 1 --pretty=format:"%%H""
for /f %%i in (' %cmd% ') do set SPARK_VERSION=%%i
set SPARK_VERSION=2.4.4-%SPARK_VERSION:~0,8%
echo "Spark Version for this build: %SPARK_VERSION%"