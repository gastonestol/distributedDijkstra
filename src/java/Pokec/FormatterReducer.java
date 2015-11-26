package Pokec;

/**
 * Created by gaston on 25/11/15.
 */

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Output Format: nodeID<tab>list_of_adjacent_nodes|distance_from_the_source|color|parent
 *
 */
public class FormatterReducer extends Reducer<Text, Text, Text, Text> {


    public Node reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        Node node = new Node();
        node.setId(key.toString());
        if(key.toString().equals(context.getConfiguration().get("source"))){
            node.setColor(Node.Color.GRAY);
            node.setDistance(0);
        }else{
            node.setColor(Node.Color.WHITE);
            node.setDistance(Integer.MAX_VALUE);
        }

        for (IntWritable val : values){
            node.setEdge(val.toString());
        }
        context.write(key,new Text(node.getNodeInfo()));
        return node;
    }

}
