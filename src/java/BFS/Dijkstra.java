package BFS;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;

import java.io.IOException;

/**
 * Created by gaston on 27/11/15.
 */
public class Dijkstra extends Configured implements Tool {

    public static String OUT = "outfile";

    public static String IN = "inputlarger";

    public static class TheMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            Text word = new Text();

            String line = value.toString();//looks like 1 0 2:3:

            String[] sp = line.split(" ");//splits on space

            int distanceadd = Integer.parseInt(sp[1]) + 1;

            String[] PointsTo = sp[2].split(":");

            for (int i = 0; i < PointsTo.length; i++) {

                word.set("VALUE " + distanceadd);//tells me to look at distance value

                context.write(new LongWritable(Integer.parseInt(PointsTo[i])), word);

                word.clear();
            }

            //pass in current node's distance (if it is the lowest distance)

            word.set("VALUE " + sp[1]);

            context.write(new LongWritable(Integer.parseInt(sp[0])), word);

            word.clear();

            word.set("NODES " + sp[2]);//tells me to append on the final tally

            context.write(new LongWritable(Integer.parseInt(sp[0])), word);

            word.clear();

        }

    }
    public static class TheReducer extends Reducer<LongWritable, Text, LongWritable, Text> {

        public void reduce(LongWritable key, Iterable<Text> values, Context context) throws

                IOException, InterruptedException {

            String nodes = "UNMODED";

            Text word = new Text();

            int lowest = 10009;//start at infinity

            for (Text val : values) {//looks like NODES/VALUES 1 0 2:3:, we need to use the first

                //as a key

                String[] sp = val.toString().split(" ");//splits on space

                //look at first value

                if(sp[0].equalsIgnoreCase("NODES")){

                    nodes = null;

                    nodes = sp[1];

                }else if(sp[0].equalsIgnoreCase("VALUE")){

                    int distance = Integer.parseInt(sp[1]);

                    lowest = Math.min(distance, lowest);

                }

            }

            word.set(lowest+" "+nodes);

            context.write(key, word);

            word.clear();

        }

    }
    public int run(String[] args) throws Exception {

        //http://code.google.com/p/joycrawler/source/browse/NetflixChallenge/src/org/niubility/learning/knn/KNNDriver.java?r=242

        getConf().set("mapred.textoutputformat.separator", " ");
        //make the key -> value space separated (for iterations)


        while (isdone == false) {

            Job job = new Job(getConf());

            job.setJarByClass(Dijkstra.class);

            job.setJobName("Dijkstra");

            job.setOutputKeyClass(LongWritable.class);

            job.setOutputValueClass(Text.class);

            job.setMapperClass(TheMapper.class);

            job.setReducerClass(TheReducer.class);

            job.setInputFormatClass(TextInputFormat.class);

            job.setOutputFormatClass(TextOutputFormat.class);

            FileInputFormat.addInputPath(job, new Path(infile));

            FileOutputFormat.setOutputPath(job, new Path(outputfile));

            success = job.waitForCompletion(true);

            //remove the input file

            //http://eclipse.sys-con.com/node/1287801/mobile

            if (infile != IN) {

                String indir = infile.replace("part-r-00000", "");

                Path ddir = new Path(indir);

                FileSystem dfs = FileSystem.get(getConf());

                dfs.delete(ddir, true);

            }
        }
    }
}