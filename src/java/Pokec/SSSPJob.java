package Pokec;

import org.apache.hadoop.conf.Configuration;
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




    // method to set the configuration for the job and the mapper and the reducer classes
    private Job getJobFormatterConf() throws Exception {

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
                return FormatterMapper.class;
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
                return FormatterReducer.class;
            }
        };

        return setupJob("formatterJob", jobInfo);


    }


    // the driver to execute the job and invoke the map/reduce functions

    public int run(String[] args) throws Exception {

        long formattingPhase = 0, ssspPhrase = 0;

        String auxiliaryFile = new Path(new Path(args[0]).getParent().getName()+"/formattedFile").getName();
        formattingPhase = formatterJob(args[0],auxiliaryFile,args[1]);

       // ssspPhrase = ssspJob(args,auxiliaryFile, args[2]);


        return 0;

    }

    public long formatterJob( String inputPath, String outputPath,String sourceNode)
            throws Exception {

        Job job = getJobFormatterConf();
        job.getConfiguration().set("source", sourceNode);
        FileInputFormat.setInputPaths(job, new Path(inputPath)); // setting the input files for the job
        FileOutputFormat.setOutputPath(job, new Path(outputPath)); // setting the output files for the job
        job.waitForCompletion(true); // wait for the job to complete

        return 0;

    }



    public static void main (String[] args) throws Exception {

        int res = ToolRunner.run(new Configuration(), new SSSPJob(), args);
        if(args.length != 2){
            System.err.println("Usage: <input dir> <source node> ");
        }
        System.exit(res);
    }


}