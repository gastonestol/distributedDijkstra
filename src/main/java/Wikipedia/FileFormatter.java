package main.java.Wikipedia;

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
 * Created by gestol on 15/3/16.
 */
// format wikipedia file to be used in scala graph loader

public class FileFormatter extends ExampleBaseJob {


    @Override
    public void ssspJob(String inputFile, String outputFile, String sourceNode) throws Exception {

    }

    public static class FormatMapper extends Mapper<Object, Text, Text, Text> {

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String [] row = value.toString().split(":");

            if(row.length>0){
                String nodeId = row[0];
                if(row.length>1) {
                    String[] neighbors = row[1].split(" ");
                    for(String neighbor : neighbors){
                        context.write(new Text(nodeId),new Text(neighbor));
                    }
                }
            }


        }
    }
    public static class FormatReducer extends Reducer<Text, Text, Text, Text> {


        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            for(Text val : values)
            context.write(key,val);
        }
    }

    private Job getFormatterJobConfigutation() throws Exception {
        JobInfo jobInfo = new JobInfo() {
            @Override
            public Class<? extends Reducer> getCombinerClass() {
                return null;
            }

            @Override
            public Class<?> getJarByClass() {
                return FileFormatter.class;
            }

            @Override
            public Class<? extends Mapper> getMapperClass() {
                return FormatMapper.class;
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
                return FormatReducer.class;
            }
        };

        return setupJob("ssspjob", jobInfo);
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

    @Override
    public int run(String[] strings) throws Exception {
        formatterJob(strings[0],strings[1]);


        return 0;
    }
    public static void main (String[] args) throws Exception {



        if(args.length < 1) {
            System.err.println("Usage: <input file> <output file>");
        }else{
            System.out.println("Usage: <input file> <output file>");
            int res = ToolRunner.run(new Configuration(), new FileFormatter(), args);
            System.exit(res);

        }
    }
}
