Old time build,



Must be done in order not to have any problems with hadoop libs, don't worry is not dificult
Remember to create the MANIFEST file not telling the main class
Code the full name of the main class after the .jar when running
BEFORE RUNNING FOR GOD'S SAKE HAVE HADOOP AND YARN RUNNING!!!

sbin/start-dfs.sh
sbin/start-yarn.sh


An Example

 COMPILE
 $HADOOP_HOME/bin/hadoop com.sun.tools.javac.Main    $DIJSKTRA_HOME/src/java/Pokec/*.java
 grails compile
 CREATE JAR:
 jar cfm program.jar META-INF/MANIFEST.MF  Pokec/*.class
 RUN:
 $HADOOP_HOME/bin/hadoop jar $DIJSKTRA_HOME/out/production/distributedDijkstra/program.jar Pokec.SSSPJob /user/gaston/pokecInput /user/gaston/pokecOutput


Common problems:

No name node starting
 solution ----->  namenode format :(

  COMPILE
  $HADOOP_HOME/bin/hadoop com.sun.tools.javac.Main    $DIJSKTRA_HOME/src/java/Pokec/*.java
  CREATE JAR:
  jar cfm program.jar META-INF/MANIFEST.MF  Pokec/*.class
  RUN:
  $HADOOP_HOME/bin/hadoop jar $DIJSKTRA_HOME/out/production/distributedDijkstra/program.jar Wikipedia.SSSPJob /user/gestol/simplegraph.txt 1 /user/gestol/ssspresult.txt 2

  $HADOOP_HOME/bin/hadoop jar $DIJSKTRA_HOME/out/production/distributedDijkstra/program.jar Wikipedia.SSSPJob /user/gaston/ssspinput 1 /user/gaston/ssspoutput 2
