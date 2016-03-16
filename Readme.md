# Distributed Dijkstra #

## Purpose ##

Benchmarking of Hadoop and Spark using SSSP Algorithm invented by Dijkstra

Source files are the hole English Wikipedia Site Graph and Pokec SocialNetwork relationships

## Mock files ##

Mock imputs for Spark and Hadoop can be found at the sample_files directory

Spark: simple_graph_spark.txt
Hadoop: 

Those files must be uploaded to HDFS before running

## Compile ##

Hadoop (Java code): mvn package

Spark (Scala code): sbt package

## Programs ##

Hadoop: target/SSSP-1.0.jar

Spark: target/scala-2.10/simplesssp_2.10-1.0.jar

## Enviroment Set up ##


### Requiments ###

If you don't have a Hadoop and/or Spark cluster go to the folowing links and read the tutorials

To run a functional mock test you need to install Hadoop and Spark 

* Hadoop Single Node Cluster: https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/SingleCluster.html

* Spark Standalone: http://spark.apache.org/docs/latest/spark-standalone.html

Also I recomend to set up the HADOOP_HOME and SPARK_HOME enviroment variables

### Run Hadoop ###

navigate to hadoop installation directory and run the folowing

sbin/start-dfs.sh

### Run Spark ###

navigate to spark installation directory and run the folowing

sbin/start-all.sh 

## Runtime ##

### Hadoop ###

Input must be stored in HDFS
Output is going to be created in HDFS

#### Wikipedia SSSP
 
 Input file must folow the format of the mock file "simple_graph"

`$HADOOP_HOME/bin/hadoop jar target/SSSP-1.0.jar main.java.Wikipedia.SSSPJob <inputfile> <source node> <outputfile>`

#### Pokec SSSP

Input file must folow the format of the mock file "simple_graph_spark"

`$HADOOP_HOME/bin/hadoop jar target/SSSP-1.0.jar main.java.Pokec.SSSPJob <inputfile> <source node> <outputfile>`

### Spark ###

Input must be stored in HDFS
Output is the log file

Input file must folow the format of the mock file "simple_graph_spark"

`$SPARK_HOME/bin/spark-submit --class "SimpleSSSP"  --master local[4] target/scala-2.10/simplesssp_2.10-1.0.jar <inputfile>`

## Contact

Write to me at _gastonestol@gmail.com_