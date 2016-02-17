package Commons;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


/**
 *
 * Description : Reducer class that implements the reduce part of parallel Breadth-first search
 *         algorithm. 
 *         Make a new node which combines all information for this single node id that is for each key. The new node should have the full list of edges, the minimum distance, the darkest Color, and the parent/predecessor node 
 *
 * Input format <key,value> : <nodeId, list_of_adjacent_nodes|distance_from_the_source>
 *
 * Output format <key,value> : <nodeId, (updated) list_of_adjacent_nodes|distance_from_the_source>
 *
 *
 */

// the type parameters are the input keys type, the input values type, the
// output keys type, the output values type

public class SearchReducer extends Reducer<Text, Text, Text, Text> {

    //the parameters are the types of the input key, the values associated with the key, the Context object through which the Reducer communicates with the Hadoop framework and the node whose information has to be output
    //the return type is a Node
    public void reduce(Text key, Iterable<Text> values, Context context, Node outNode) throws IOException, InterruptedException {

        Integer minimunDistance = Integer.MAX_VALUE;
        for(Text value : values) {

            if (Node.isNode(value.toString())) {
               // System.out.println("Is node " + key + " value: " +value.toString());

                outNode = new Node(value.toString());
                outNode.setId(key.toString());
                if(minimunDistance > outNode.getDistance())
                    minimunDistance = outNode.getDistance();
                //System.out.println("Is node " + key + " value: " +value.toString()+" info: "+ outNode.getNodeInfo());
            }
            else{
                //System.out.println("node " + key + " distance value " + value.toString());


                if (!value.toString().trim().equals("Integer.MAX_VALUE") && Integer.valueOf(value.toString().trim()) < minimunDistance)
                    minimunDistance = Integer.valueOf(value.toString().trim());

               //System.out.println("node " + key + " minimunDistance " + minimunDistance);
            }
            //System.out.println("node " + key + " current minumun distance " + minimunDistance);
        }
        outNode.setDistance(minimunDistance);


        //emit the key, value pair where the key is the node id and the value is the node's information

        //System.out.println("Reducer: Emiting "+key+" "+ outNode.getNodeInfo());

        context.write(key, new Text(outNode.getNodeInfo()));


    }
}