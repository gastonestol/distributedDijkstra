package Pokec;

/**
 * Created by gaston on 25/11/15.
 */

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Input format node_from<tab>node_to
 */
public class FormatterMapper extends Mapper<Object, Text, Text, Text> {


    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        String [] nodes = value.toString().split("\t");
        //Node node = new Node(value.toString());
        context.write(new Text(nodes[0]), new Text(nodes[1]));

    }


}