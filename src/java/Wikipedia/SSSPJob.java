package Wikipedia;



import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;


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

            //calls the map method of the super class SearchMapper
            super.map(key, value, context);

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

            //create a new out node and set its values
            Node outNode = new Node();

            //call the reduce method of SearchReducer class
            outNode = super.reduce(key, values, context, outNode);


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
                return Wikipedia.SSSPJob.class;
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

        Job job = getJobSsspConfiguration();
        job.getConfiguration().set("source", sourceNode);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        FileInputFormat.setInputPaths(job, new Path(inputPath)); // setting the input files for the job
        FileOutputFormat.setOutputPath(job, new Path(outputPath)); // setting the output files for the job
        job.waitForCompletion(true); // wait for the job to complete

        return 0;

    }

    // the driver to execute the job and invoke the map/reduce functions

    public int run(String[] args) throws Exception {

        long formattingPhase = 0, ssspPhrase = 0;



        String auxiliaryFile = new Path(new Path(args[0]).getParent().getName()+"/formattedFile").getName();
        FileSystem fs = FileSystem.get(getConf());
        if(fs.exists(new Path(auxiliaryFile)))
            fs.delete(new Path(auxiliaryFile), true);

        ssspPhrase = ssspJob(args[0], args[2],args[1]);






        return 0;

    }



    public static void main (String[] args) throws Exception {

        int res = ToolRunner.run(new Configuration(), new Pokec.SSSPJob(), args);
        if(args.length != 3){
            System.err.println("Usage: <input dir> <source node> <output di>");
        }
        System.exit(res);
    }


}