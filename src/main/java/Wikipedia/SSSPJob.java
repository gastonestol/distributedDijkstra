package Wikipedia;



import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Date;
import Commons.*;

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

public class SSSPJob extends ExampleBaseJob {


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
            if(Node.isNode(value.toString())){
                Node inNode = new Node(value.toString());
                //System.out.println("processing node id: "+inNode.getId()+ " source "+context.getConfiguration().get("source")+ "equals: "+inNode.getId().trim().equals(context.getConfiguration().get("source")));
                if(inNode.getId().trim().equals(context.getConfiguration().get("source"))){

                    inNode.setDistance(0);
                    inNode.setParent(Node.SOURCE);
                    inNode.setColor(Node.Color.GRAY);
                }
                //calls the map method of the super class SearchMapper
                super.map(key, value, context, inNode);
            }


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

    public static class SearchReducerSSSP extends SearchReducer {


        //the parameters are the types of the input key, the values associated with the key and the Context object through which the Reducer communicates with the Hadoop framework


        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            Node outNode = new Node();
            //call the reduce method of SearchReducer class
            super.reduce(key, values, context, outNode);
            if (outNode.getColor() == Node.Color.GRAY)
                context.getCounter(MoreIterations.numberOfIterations).increment(1L);

        }
    }



    private Job getJobSsspConfiguration() throws Exception {
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

    public long ssspJob( String inputPath, String outputPath,String sourceNode)
            throws Exception {

        int iterationCount = 0; // counter to set the ordinal number of the intermediate outputs
        long terminationValue =1;
        Date start = new Date();
        long totalTime = 0;
        Job job;
        // while there are more gray nodes to process

        while(terminationValue >0){

            Date iterStart = new Date();
            job = getJobSsspConfiguration(); // get the job configuration
            job.getConfiguration().set("source", sourceNode.trim());
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

            String input, output;

            if (iterationCount == 0){
                FileInputFormat.setInputPaths(job, new Path(inputPath)); // setting the input files for the job
                FileOutputFormat.setOutputPath(job, new Path(outputPath+"_"+iterationCount)); // setting the output files for the job

            }else{
                FileInputFormat.setInputPaths(job, new Path(outputPath+"_"+(iterationCount-1))); // setting the input files for the job
                FileOutputFormat.setOutputPath(job, new Path(outputPath+"_"+iterationCount)); // setting the output files for the job

            }

            job.waitForCompletion(true); // wait for the job to complete
            Date iterEnd = new Date();

            System.out.println("Job number " + iterationCount + " length in ms: " + (iterEnd.getTime() - iterStart.getTime()));

            Counters jobCntrs = job.getCounters();
            terminationValue = jobCntrs.findCounter(MoreIterations.numberOfIterations).getValue();//if the counter's value is incremented in the reducer(s), then there are more GRAY nodes to process implying that the iteration has to be continued.



            iterationCount++;
        }
        Date end = new Date();

        System.out.println("Complete Job length in ms: "+(end.getTime()-start.getTime()));
        return 0;

    }

    // the driver to execute the job and invoke the map/reduce functions

    public int run(String[] args) throws Exception {


        ssspJob(args[0], args[2],args[1]);


        return 0;

    }



    public static void main (String[] args) throws Exception {



        if(args.length < 3) {
            System.err.println("Usage: <input dir> <source node> <output di> <diameter>");
        }else{
            System.out.println("Usage: <input dir> <source node> <output di> <diameter>");
            int res = ToolRunner.run(new Configuration(), new SSSPJob(), args);
            System.exit(res);

        }
    }


}