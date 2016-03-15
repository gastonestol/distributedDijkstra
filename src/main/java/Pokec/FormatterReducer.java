package Pokec;

/**
 * Created by gaston on 25/11/15.
 */

import Commons.Node;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Output Format: nodeID<tab>list_of_adjacent_nodes|distance_from_the_source|color|parent
 *
 */
public class FormatterReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        Node node = new Node();
        node.setId(key.toString());
        /*if(key.toString().equals(context.getConfiguration().get("source"))){

            node.setDistance(0);
        }else{
            node.setDistance(Integer.MAX_VALUE);

        }*/

        for (Text val : values){
            node.setEdge(val.toString());
        }
        //System.out.println("Emiting key: "+key.toString()+" value: "+node.getNodeInfo());
        context.write(key,new Text(node.getNodeInfo()));
    }

}
