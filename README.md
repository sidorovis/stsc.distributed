# stsc.distributed
Project that will unite both hadoop / spark distributed calculation mechanisms.

# stsc.distributed.spark
This is example project to make possibility to use Spark as distribution mechanism for calculation.

# stsc.distributed.hadoop

Require additional actions:
```
mvn install:install-file -DgroupId=jdk.tools -DartifactId=jdk.tools -Dpackaging=jar -Dversion=1.6 -Dfile=tools.jar -DgeneratePom=true
```
```
git clone https://github.com/sidorovis/HadoopOnWindows.git
copy dll/bin to <HADOOP_HOME>/bin/ folder
setup environment variable HADOOP_HOME to it.
restart eclipse
```

