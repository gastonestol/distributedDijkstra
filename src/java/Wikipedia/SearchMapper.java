

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
    public void map(Object key, Text value, Context context, WikipediaNode inWikipediaNode) throws IOException, InterruptedException {


        if(inWikipediaNode.getId().equals(context.getConfiguration().get("source").trim())){
            inWikipediaNode.setDistance(0);
        }
        System.out.println("Mapper: Emiting "+ inWikipediaNode.getId()+" "+ inWikipediaNode.getNodeInfo());

        context.write(new Text(inWikipediaNode.getId()),new Text( inWikipediaNode.getNodeInfo()));
        for(String edge : inWikipediaNode.getEdges()){
            WikipediaNode adjacentWikipediaNode = new WikipediaNode();
            adjacentWikipediaNode.setId(edge);
            adjacentWikipediaNode.setDistance(inWikipediaNode.getDistance()+1);
            System.out.println("Mapper: Emiting " + adjacentWikipediaNode + " " + String.valueOf(adjacentWikipediaNode.getDistance()));
            if(inWikipediaNode.getDistance() == Integer.MAX_VALUE){
                context.write(new Text(adjacentWikipediaNode.getId()),new Text("Integer.MAX_VALUE"));
            }else{
                context.write(new Text(adjacentWikipediaNode.getId()),new Text(String.valueOf(adjacentWikipediaNode.getDistance())));
            }
        }






    }
}