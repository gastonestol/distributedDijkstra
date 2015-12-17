

package Wikipedia;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 *
 * @author Deepika Mohan
 */

/**
 *
 * Description : Mapper class that implements the map part of  Breadth-first search algorithm. The nodes colored WHITE or BLACK are emitted as such. For each node that is colored GRAY, a new
 * node is emitted with the distance incremented by one and the color set to GRAY. The original GRAY colored node is set to BLACK color and it is also emitted.
 *
 * Input format <key, value> : <line offset in the input file (automatically assigned), nodeID<tab>list_of_adjacent_nodes|distance_from_the_source|color|parent>
 *
 * Output format <key, value> : <nodeId, (updated) list_of_adjacent_nodes|distance_from_the_source|color|parent node>
 *
 * Reference : http://www.johnandcailin.com/blog/cailin/breadth-first-graph-search-using-iterative-map-reduce-algorithm
 *
 *
 */

// the type parameters are the input keys type, the input values type, the
// output keys type, the output values type
public class SearchMapper extends Mapper<Object, Text, Text, Text> {

    //the parameters are the types of the input key, input value and the Context object through which the Mapper communicates with the Hadoop framework
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        Node node = new Node(value.toString());

        if(key.toString().equals(context.getConfiguration().get("source"))){
            node.setDistance(0L);
        }else{
            node.setDistance(Long.MAX_VALUE);

        }
        context.write(new Text(node.getId()),new Text( node.getNodeInfo()));





    }
}