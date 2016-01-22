package Commons;

import java.util.ArrayList;
import java.util.List;

/**
 *
 *         Description : Node class to process the information about the nodes. This class contains getter and setter methods to access and
 *         set information about the nodes. The information generally includes the list of adjacent nodes, distance from the source, color of the node
 *         and the parent/predecessor node.
 *
 *
 *         Reference :
 *         http://www.johnandcailin.com/blog/cailin/breadth-first-graph
 *         -search-using-iterative-map-reduce-algorithm
 *
 *         changes made from the source code in the reference: parent node field is added.
 *
 *         Hadoop version used : 0.20.2
 */
public class Node {

    public static boolean isNode(String nodeInfo) {


        return nodeInfo.contains(":");


    }

    // three possible colors a node can have (to keep track of the visiting status of the nodes during graph search)



    private String id; // id of the node
    private int distance; // distance of the node from the source
    private List<String> edges = new ArrayList<String>(); // list of edges

    public Node() {

        distance = Integer.MAX_VALUE;

    }


    // constructor
    //the parameter nodeInfo  is the line that is passed from the input, this nodeInfo is then split into key, value pair where the key is the node id
    //and the value is the information associated with the node

    //nodeId:(edges):distanceToSource
    //2: 3 747213 1664968 1691047 4095634 5535664:distanceToSource

    public Node(String nodeInfo) {



        String []inputLine =  nodeInfo.split("\\|");
        if(inputLine.length>1){
            if (inputLine[1].trim().equals("Integer.MAX_VALUE")) {
                this.distance = Integer.MAX_VALUE;
            } else {
                this.distance = Integer.parseInt(inputLine[1].trim());
            }
        }
        else{
            this.distance = Integer.MAX_VALUE;
        }
        String []keyAndEdges = inputLine[0].split(":");
        String key ="",value ="";

        try {
            key = keyAndEdges[0].trim(); // node id
            this.id = key; // set the id of the node

        } catch (Exception e) {
            //System.out.println("inputLine: "+inputLine[0]);
            //System.out.println("node info: "+nodeInfo);
            //System.out.println("key " +key);
            //e.printStackTrace();
            //System.exit(1);

        }
        if(keyAndEdges.length>1){
            value = keyAndEdges[1]; // the list of adjacent nodes, distance, color, parent
            String[] tokens = value.split(" "); // split the value into tokens where
            //tokens[0] = list of adjacent nodes, tokens[1]= distance, tokens[2]= color, tokens[3]= parent


            // setting the edges of the node
            for (String s : tokens) {
                if (s.length() > 0) {
                    edges.add(s);
                }
            }
        }


    }

    // this method appends the list of adjacent nodes, the distance , the color and the parent and returns all these information as a single Text
    public String getNodeInfo() {
        StringBuffer s = new StringBuffer();
        s.append(":");
        // forms the list of adjacent nodes by separating them using ' '
        try {
            for (String v : edges) {
                s.append(v).append(" ");
            }
        } catch (NullPointerException e) {
            e.printStackTrace();
            System.exit(1);
        }

        // after the list of edges, append '|'
        s.append("|");

        // append the minimum distance between the current distance and
        // Integer.Max_VALUE
        if (this.distance < Integer.MAX_VALUE) {
            s.append(this.distance);
        } else {
            s.append("Integer.MAX_VALUE");
        }

        return s.toString();
    }

    // getter and setter methods

    public String getId() {
        return this.id;
    }

    public Integer getDistance() {
        return this.distance;
    }

    public void setId(String id) {
        this.id = id;
    }

    public void setDistance(Integer distance) {
        this.distance = distance;
    }

    public List<String> getEdges() {
        return this.edges;
    }

    public void setEdge(String nodeId){this.edges.add(nodeId); }

    public void setEdges(List<String> edges) {
        this.edges = edges;
    }


}