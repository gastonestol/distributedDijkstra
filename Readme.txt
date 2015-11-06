
 COMPILE bin/hadoop com.sun.tools.javac.Main    $DIJSKTRA_HOME/src/java/HadoopGTK/Intersect.java $DIJSKTRA_HOME/src/java/HadoopGTK/ExampleBaseJob.java
 CREATE JAR: $DIJSKTRA_HOME/dijsktraBFShadoop/out/production/dijsktraBFShadoop jar cfm program.jar META-INF/MANIFEST.MF  HadoopGTK/*.class
 RUN: bin/hadoop jar $DIJSKTRA_HOME/dijsktraBFShadoop/out/production/dijsktraBFShadoop/program.jar HadoopGTK.Intersect /user/gestol/intersectInput /user/gestol/output
