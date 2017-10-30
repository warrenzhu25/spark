@echo Install jars for building mtdistcp submodule in YarnForAP

rem Set constants
set GROUP_ID=org.apache.spark
set SPARK_NAO=2.1.1-nao
set JAR_BASE_DIR=assembly\target\scala-2.11\jars

rem Install pom files
call mvn install:install-file -Dfile=pom.xml -DgroupId=%GROUP_ID% -DartifactId=spark-parent_2.11 -Dversion=%SPARK_NAO% -Dpackaging=pom
call mvn install:install-file -Dfile=core\dependency-reduced-pom.xml -DgroupId=%GROUP_ID% -DartifactId=spark-core_2.11 -Dversion=%SPARK_NAO% -Dpackaging=pom

rem Install jar files
call mvn install:install-file -Dfile=%JAR_BASE_DIR%\spark-core_2.11-%SPARK_NAO%.jar -DgroupId=%GROUP_ID% -DartifactId=spark-core_2.11 -Dversion=%SPARK_NAO% -Dpackaging=jar
call mvn install:install-file -Dfile=%JAR_BASE_DIR%\spark-launcher_2.11-%SPARK_NAO%.jar -DgroupId=%GROUP_ID% -DartifactId=spark-launcher_2.11 -Dversion=%SPARK_NAO% -Dpackaging=jar
call mvn install:install-file -Dfile=%JAR_BASE_DIR%\spark-network-common_2.11-%SPARK_NAO%.jar -DgroupId=%GROUP_ID% -DartifactId=spark-network-common_2.11 -Dversion=%SPARK_NAO% -Dpackaging=jar
call mvn install:install-file -Dfile=%JAR_BASE_DIR%\spark-network-shuffle_2.11-%SPARK_NAO%.jar -DgroupId=%GROUP_ID% -DartifactId=spark-network-shuffle_2.11 -Dversion=%SPARK_NAO% -Dpackaging=jar
call mvn install:install-file -Dfile=%JAR_BASE_DIR%\spark-tags_2.11-%SPARK_NAO%.jar -DgroupId=%GROUP_ID% -DartifactId=spark-tags_2.11 -Dversion=%SPARK_NAO% -Dpackaging=jar
call mvn install:install-file -Dfile=%JAR_BASE_DIR%\spark-unsafe_2.11-%SPARK_NAO%.jar -DgroupId=%GROUP_ID% -DartifactId=spark-unsafe_2.11 -Dversion=%SPARK_NAO% -Dpackaging=jar

@echo Done
