package main.java.Pokec;



import main.java.Commons.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Date;


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




    private Job getFormatterJobConfigutation() throws Exception {



        JobInfo jobInfo = new JobInfo() {

            @Override
            public Class<? extends Reducer> getReducerClass() {

                return FormatterReducer.class;
            }

            @Override
            public Class<?> getOutputValueClass() {

                return Text.class;
            }

            @Override
            public Class<?> getOutputKeyClass() {

                return Text.class;
            }

            @Override
            public Class<? extends Mapper> getMapperClass() {

                return FormatterMapper.class;
            }

            @Override
            public Class<?> getJarByClass() {
                return SSSPJob.class;
            }

            @Override
            public Class<? extends Reducer> getCombinerClass() {
                return null;
            }
        };

        return setupJob("formatterjob", jobInfo);


    }



    public void formatterJob(String inputPath, String outputPath)   throws Exception {
        Job job = getFormatterJobConfigutation();
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        FileInputFormat.setInputPaths(job, new Path(inputPath)); // setting the input files for the job
        FileOutputFormat.setOutputPath(job, new Path(outputPath)); // setting the output files for the job

        System.out.println("Starting Formatter Job");

        Date start = new Date();

        job.waitForCompletion(true); // wait for the job to complete

        Date end = new Date();

        System.out.println("Formatter Job length in ms: "+(end.getTime()-start.getTime()));


    }

    // the driver to execute the job and invoke the map/reduce functions

    public int run(String[] args) throws Exception {



        String temporalFile = "formatted_"+args[0];
        formatterJob(args[0],temporalFile);
        ExampleBaseJob ssspJob = new main.java.Wikipedia.SSSPJob();
        ssspJob.ssspJob(temporalFile, args[2], args[1]);


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


    @Override
    public void ssspJob(String inputFile, String outputFile, String sourceNode) throws Exception {

    }
}