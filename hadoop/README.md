Require additional actions:
```
mvn install:install-file -DgroupId=jdk.tools -DartifactId=jdk.tools -Dpackaging=jar -Dversion=1.6 -Dfile=tools.jar -DgeneratePom=true
```
```
git clone https://github.com/sidorovis/HadoopOnWindows.git
setup environment variable HADOOP_HOME to it.
copy dll/exe/binary files to <HADOOP_HOME>/bin/ folder
restart eclipse
```

