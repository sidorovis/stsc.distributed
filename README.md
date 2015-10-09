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

