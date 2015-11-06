
 COMPILE $HADOOP_HOME/bin/hadoop com.sun.tools.javac.Main    $DIJSKTRA_HOME/src/java/HadoopGTK/*.java
 CREATE JAR:  jar cfm program.jar META-INF/MANIFEST.MF  HadoopGTK/*.class
 RUN:  $HADOOP_HOME/bin/hadoop jar $DIJSKTRA_HOME/out/production/dijsktraBFShadoop/program.jar HadoopGTK.SSSPJob /user/gestol/ssspinput /user/gestol/ssspoutput
