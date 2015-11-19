Old time build,



Must be done in order not to have any problems with hadoop libs, don't worry is not dificult
Remember to create the MANIFEST file not telling the main class
Code the full name of the main class after the .jar when running
BEFORE RUNNING FOR GOD'S SAKE HAVE HADOOP AND YARN RUNNING!!!

sbin/start-dfs.sh
sbin/start-yarn.sh


An Example

 COMPILE
 $HADOOP_HOME/bin/hadoop com.sun.tools.javac.Main    $DIJSKTRA_HOME/src/java/HadoopGTK/*.java
 CREATE JAR:
 jar cfm program.jar META-INF/MANIFEST.MF  HadoopGTK/*.class
 RUN:
 $HADOOP_HOME/bin/hadoop jar $DIJSKTRA_HOME/out/production/dijsktraBFShadoop/program.jar HadoopGTK.SSSPJob /user/gaston/intersectInput /user/gaston/intersectOutput


Common problems:

No name node starting
 solution ----->  namenode format :(