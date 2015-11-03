import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;



/**
 *
 * @author Deepika Mohan
 *
 * Description : MapReduce program to solve the single-source shortest path problem using parallel breadth-first search. This program also illustrates how to perform iterative map-reduce.
 *
 *
 * The single source shortest path is implemented by using Breadth-first search concept.
 *
 * Reference : http://www.johnandcailin.com/blog/cailin/breadth-first-graph-search-using-iterative-map-reduce-algorithm
 *
 * Hadoop version used : 0.20.2
 */

public class SSSPJob extends Configured implements Tool{//extends ExampleBaseJob {

    //counter to determine the number of iterations or if more iterations are required to execute the map and reduce functions

    static enum MoreIterations {
        numberOfIterations
    }

    /**
     *
     * Description : Mapper class that implements the map part of Single-source shortest path algorithm. It extends the SearchMapper class.
     * The map method calls the super class' map method.
     *  Input format : nodeID<tab>list_of_adjacent_nodes|distance_from_the_source|color|parent

     *
     * Reference : http://www.johnandcailin.com/blog/cailin/breadth-first-graph-search-using-iterative-map-reduce-algorithm
     *
     *
     */
    public static class SearchMapperSSSP extends SearchMapper {


        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            Node inNode = new Node(value.toString());
            //calls the map method of the super class SearchMapper
            super.map(key, value, context, inNode);

        }
    }
    /**
     *
     * Description : Reducer class that implements the reduce part of the Single-source shortest path algorithm. This class extends the SearchReducer class that implements parallel breadth-first search algorithm.
     *      The reduce method implements the super class' reduce method and increments the counter if the color of the node returned from the super class is GRAY.
     *

     *
     */

    // the type parameters are the input keys type, the input values type, the
    // output keys type, the output values type

    public static class SearchReducerSSSP extends SearchReducer{


        //the parameters are the types of the input key, the values associated with the key and the Context object through which the Reducer communicates with the Hadoop framework


        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            //create a new out node and set its values
            Node outNode = new Node();

            //call the reduce method of SearchReducer class
            outNode = super.reduce(key, values, context, outNode);

            //if the color of the node is gray, the execution has to continue, this is done by incrementing the counter
            if (outNode.getColor() == Node.Color.GRAY)
                context.getCounter(MoreIterations.numberOfIterations).increment(1L);
        }
    }


    // method to set the configuration for the job and the mapper and the reducer classes
    private Job getJobConf(String[] args) throws Exception {

        JobInfo jobInfo = new JobInfo() {
            @Override
            public Class<? extends Reducer> getCombinerClass() {
                return null;
            }

            @Override
            public Class<?> getJarByClass() {
                return SSSPJob.class;
            }

            @Override
            public Class<? extends Mapper> getMapperClass() {
                return SearchMapperSSSP.class;
            }

            @Override
            public Class<?> getOutputKeyClass() {
                return Text.class;
            }

            @Override
            public Class<?> getOutputValueClass() {
                return Text.class;
            }

            @Override
            public Class<? extends Reducer> getReducerClass() {
                return SearchReducerSSSP.class;
            }
        };

        return setupJob("ssspjob", jobInfo);


    }

    // the driver to execute the job and invoke the map/reduce functions

    public int run(String[] args) throws Exception {

        int iterationCount = 0; // counter to set the ordinal number of the intermediate outputs

        Job job;

        long terminationValue =1;


        // while there are more gray nodes to process

        while(terminationValue >0){

            job = getJobConf(args); // get the job configuration
            String input, output;

            //setting the input file and output file for each iteration
            //during the first time the user-specified file will be the input whereas for the subsequent iterations
            // the output of the previous iteration will be the input
            if (iterationCount == 0) // for the first iteration the input will be the first input argument
                input = args[0];
            else
                // for the remaining iterations, the input will be the output of the previous iteration
                input = args[1] + iterationCount;

            output = args[1] + (iterationCount + 1); // setting the output file

            FileInputFormat.setInputPaths(job, new Path(input)); // setting the input files for the job
            FileOutputFormat.setOutputPath(job, new Path(output)); // setting the output files for the job

            job.waitForCompletion(true); // wait for the job to complete

            Counters jobCntrs = job.getCounters();
            terminationValue = jobCntrs.findCounter(MoreIterations.numberOfIterations).getValue();//if the counter's value is incremented in the reducer(s), then there are more GRAY nodes to process implying that the iteration has to be continued.
            iterationCount++;

        }

        return 0;
    }

    public static void main(String[] args) throws Exception {

        int res = ToolRunner.run(new Configuration(), new SSSPJob(), args);
        if(args.length != 2){
            System.err.println("Usage: <in> <output name> ");
        }
        System.exit(res);
    }

    protected Job setupJob(String jobName,JobInfo jobInfo) throws Exception {


        Job job = new Job(new Configuration(), jobName);

        // set the several classes
        job.setJarByClass(jobInfo.getJarByClass());

        //set the mapper class
        job.setMapperClass(jobInfo.getMapperClass());

        //the combiner class is optional, so set it only if it is required by the program
        if (jobInfo.getCombinerClass() != null)
            job.setCombinerClass(jobInfo.getCombinerClass());

        //set the reducer class
        job.setReducerClass(jobInfo.getReducerClass());

        //the number of reducers is set to 3, this can be altered according to the program's requirements
        job.setNumReduceTasks(3);

        // set the type of the output key and value for the Map & Reduce
        // functions
        job.setOutputKeyClass(jobInfo.getOutputKeyClass());
        job.setOutputValueClass(jobInfo.getOutputValueClass());

        return job;
    }

    protected abstract class JobInfo {
        public abstract Class<?> getJarByClass();
        public abstract Class<? extends Mapper> getMapperClass();
        public abstract Class<? extends Reducer> getCombinerClass();
        public abstract Class<? extends Reducer> getReducerClass();
        public abstract Class<?> getOutputKeyClass();
        public abstract Class<?> getOutputValueClass();

    }

    /**
     *
     *         Description : Node class to process the information about the nodes. This class contains getter and setter methods to access and
     *         set information about the nodes. The information generally includes the list of adjacent nodes, distance from the source, color of the node
     *         and the parent/predecessor node.
     *
     *
     *         Reference :
     *         http://www.johnandcailin.com/blog/cailin/breadth-first-graph
     *         -search-using-iterative-map-reduce-algorithm
     *
     *         changes made from the source code in the reference: parent node field is added.
     *
     *         Hadoop version used : 0.20.2
     */
    public static class Node {

        // three possible colors a node can have (to keep track of the visiting status of the nodes during graph search)

        public static enum Color {
            WHITE, GRAY, BLACK
        };

        private String id; // id of the node
        private int distance; // distance of the node from the source
        private List<String> edges = new ArrayList<String>(); // list of edges
        private Color color = Color.WHITE;
        private String parent; // parent/predecessor of the node : The parent of the source is marked "source" to leave it unchanged

        public Node() {


            distance = Integer.MAX_VALUE;
            color = Color.WHITE;
            parent = null;
        }

        // constructor
        //the parameter nodeInfo  is the line that is passed from the input, this nodeInfo is then split into key, value pair where the key is the node id
        //and the value is the information associated with the node
        public Node(String nodeInfo) {

            String[] inputLine = nodeInfo.split("\t"); //splitting the input line record by tab delimiter into key and value
            String key = "", value = ""; //initializing the strings 'key' and 'value'

            try {
                key = inputLine[0]; // node id
                value = inputLine[1]; // the list of adjacent nodes, distance, color, parent

            } catch (Exception e) {
                e.printStackTrace();
                System.exit(1);

            }

            String[] tokens = value.split("\\|"); // split the value into tokens where
            //tokens[0] = list of adjacent nodes, tokens[1]= distance, tokens[2]= color, tokens[3]= parent

            this.id = key; // set the id of the node

            // setting the edges of the node
            for (String s : tokens[0].split(",")) {
                if (s.length() > 0) {
                    edges.add(s);
                }
            }

            // setting the distance of the node
            if (tokens[1].equals("Integer.MAX_VALUE")) {
                this.distance = Integer.MAX_VALUE;
            } else {
                this.distance = Integer.parseInt(tokens[1]);
            }

            // setting the color of the node
            this.color = Color.valueOf(tokens[2]);

            // setting the parent of the node
            this.parent = tokens[3];

        }

        // this method appends the list of adjacent nodes, the distance , the color and the parent and returns all these information as a single Text
        public Text getNodeInfo() {
            StringBuffer s = new StringBuffer();

            // forms the list of adjacent nodes by separating them using ','
            try {
                for (String v : edges) {
                    s.append(v).append(",");
                }
            } catch (NullPointerException e) {
                e.printStackTrace();
                System.exit(1);
            }

            // after the list of edges, append '|'
            s.append("|");

            // append the minimum distance between the current distance and
            // Integer.Max_VALUE
            if (this.distance < Integer.MAX_VALUE) {
                s.append(this.distance).append("|");
            } else {
                s.append("Integer.MAX_VALUE").append("|");
            }

            // append the color of the node
            s.append(color.toString()).append("|");

            // append the parent of the node
            s.append(getParent());

            return new Text(s.toString());
        }

        // getter and setter methods

        public String getId() {
            return this.id;
        }

        public int getDistance() {
            return this.distance;
        }

        public void setId(String id) {
            this.id = id;
        }

        public void setDistance(int distance) {
            this.distance = distance;
        }

        public Color getColor() {
            return this.color;
        }

        public void setColor(Color color) {
            this.color = color;
        }

        public List<String> getEdges() {
            return this.edges;
        }

        public void setEdges(List<String> edges) {
            this.edges = edges;
        }

        public void setParent(String parent) {
            this.parent = parent;
        }

        public String getParent() {
            return parent;
        }

    }
    /**
     *
     * @author Deepika Mohan
     */

    /**
     *
     * Description : Mapper class that implements the map part of  Breadth-first search algorithm. The nodes colored WHITE or BLACK are emitted as such. For each node that is colored GRAY, a new
     * node is emitted with the distance incremented by one and the color set to GRAY. The original GRAY colored node is set to BLACK color and it is also emitted.
     *
     * Input format <key, value> : <line offset in the input file (automatically assigned), nodeID<tab>list_of_adjacent_nodes|distance_from_the_source|color|parent>
     *
     * Output format <key, value> : <nodeId, (updated) list_of_adjacent_nodes|distance_from_the_source|color|parent node>
     *
     * Reference : http://www.johnandcailin.com/blog/cailin/breadth-first-graph-search-using-iterative-map-reduce-algorithm
     *
     *
     */

// the type parameters are the input keys type, the input values type, the
// output keys type, the output values type
    static public class SearchMapper extends Mapper<Object, Text, Text, Text> {

        //the parameters are the types of the input key, input value and the Context object through which the Mapper communicates with the Hadoop framework
        public void map(Object key, Text value, Context context, Node inNode)
                throws IOException, InterruptedException {



            // For each GRAY node, emit each of the adjacent nodes as a new node (also GRAY)
            //if the adjacent node is already processed and colored BLACK, the reducer retains the color BLACK
            if (inNode.getColor() == Node.Color.GRAY) {
                for (String neighbor : inNode.getEdges()) { // for all the adjacent nodes of
                    // the gray node

                    Node adjacentNode = new Node(); // create a new node

                    adjacentNode.setId(neighbor); // set the id of the node
                    adjacentNode.setDistance(inNode.getDistance() + 1); // set the distance of the node, the distance of the adjacentNode is set to be the distance
                    //of its predecessor node+ 1, this is done since we consider a graph of unit edge weights
                    adjacentNode.setColor(Node.Color.GRAY); // set the color of the node to be GRAY
                    adjacentNode.setParent(inNode.getId()); // set the parent of the node, if the adjacentNode is already visited by some other parent node, it is not update in the reducer
                    //this is because the nodes are processed in terms of levels from the source node, i.e all the nodes in the level 1 are processed first, then the nodes in the level 2 and so on.

                    //emit the information about the adjacent node in the form of key-value pair where the key is the node Id and the value is the associated information
                    //for the nodes emitted here, the list of adjacent nodes will be empty in the value part, the reducer will merge this with the original list of adjacent nodes associated with a node
                    context.write(new Text(adjacentNode.getId()), adjacentNode.getNodeInfo());

                }
                // this node is done, color it black
                inNode.setColor(Node.Color.BLACK);
            }

            // No matter what, emit the input node
            // If the node came into this method GRAY, it will be output as BLACK
            //otherwise, the nodes that are already colored BLACK or WHITE are emitted by setting the key as the node Id and the value as the node's information
            context.write(new Text(inNode.getId()), inNode.getNodeInfo());

        }
    }
    /**
     *
     * Description : Reducer class that implements the reduce part of parallel Breadth-first search
     *         algorithm.
     *         Make a new node which combines all information for this single node id that is for each key. The new node should have the full list of edges, the minimum distance, the darkest Color, and the parent/predecessor node
     *
     * Input format <key,value> : <nodeId, list_of_adjacent_nodes|distance_from_the_source|color|parent_node>
     *
     * Output format <key,value> : <nodeId, (updated) list_of_adjacent_nodes|distance_from_the_source|color|parent_node>
     *
     *
     */

    // the type parameters are the input keys type, the input values type, the
    // output keys type, the output values type

    static public class SearchReducer extends Reducer<Text, Text, Text, Text> {

        //the parameters are the types of the input key, the values associated with the key, the Context object through which the Reducer communicates with the Hadoop framework and the node whose information has to be output
        //the return type is a Node
        public Node reduce(Text key, Iterable<Text> values, Context context, Node outNode)
                throws IOException, InterruptedException {

            //set the node id as the key
            outNode.setId(key.toString());

            //since the values are of the type Iterable, iterate through the values associated with the key
            //for all the values corresponding to a particular node id

            for (Text value : values) {

                Node inNode = new Node(key.toString() + "\t" + value.toString());

                // One (and only one) copy of the node will be the fully expanded version, which includes the list of adjacent nodes, in other cases, the mapper emits the ndoes with no adjacent nodes
                //In other words, when there are multiple values associated with the key (node Id), only one will
                if (inNode.getEdges().size() > 0) {
                    outNode.setEdges(inNode.getEdges());
                }

                // Save the minimum distance
                if (inNode.getDistance() < outNode.getDistance()) {
                    outNode.setDistance(inNode.getDistance());

                    //if the distance gets updated then the predecessor node that was responsible for this distance will be the parent node
                    outNode.setParent(inNode.getParent());
                }

                // Save the darkest color
                if (inNode.getColor().ordinal() > outNode.getColor().ordinal()) {
                    outNode.setColor(inNode.getColor());
                }

            }
            //emit the key, value pair where the key is the node id and the value is the node's information
            context.write(key, new Text(outNode.getNodeInfo()));

            return outNode;

        }
    }

}