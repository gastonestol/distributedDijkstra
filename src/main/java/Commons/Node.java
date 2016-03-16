package main.java.Commons;

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
    public static enum Color {
        WHITE, GRAY, BLACK
    };

    static private int keyAndEdgesPosition = 0;
    static private int distancePosition = 1;
    static private int colorPosition = 2;
    static private int parentPosition = 3;
    static private int keyPosition = 0;
    static private int edgesPosition = 1;
    public static String NULL = "null";
    public static String MAX_VALUE = "Integer.MAX_VALUE";
    public static String SOURCE = "source";

    // three possible colors a node can have (to keep track of the visiting status of the nodes during graph search)



    private String id; // id of the node
    private int distance; // distance of the node from the source
    private List<String> edges = new ArrayList<String>(); // list of edges
    private Color color = Color.WHITE;
    private String parent; // parent/predecessor of the node : The parent of the source is marked "source" to leave it unchanged
    public Node() {
        distance = Integer.MAX_VALUE;
        color = Color.WHITE;
        parent = null;

    }
    /**
     1<tab>2,3|0|GRAY|source
     2<tab>1,3,4,5|Integer.MAX_VALUE|WHITE|null

     1: 2 3 4 5|distance|colour|parent

    */
    // constructor
    //the parameter nodeInfo  is the line that is passed from the input, this nodeInfo is then split into key, value pair where the key is the node id
    //and the value is the information associated with the node

    //nodeId:(edges):distanceToSource
    //2: 3 747213 1664968 1691047 4095634 5535664:distanceToSource

    public Node(String nodeInfo) {



        String []inputLine =  nodeInfo.split("\\|");
        String []keyAndEdges = inputLine[keyAndEdgesPosition].split(":");
        String value ="";

        try {
            this.id =  keyAndEdges[keyPosition].trim(); // node id

        } catch (Exception e) {
            //System.out.println("inputLine: "+inputLine[0]);
            //System.out.println("node info: "+nodeInfo);
            //System.out.println("key " +key);
            //e.printStackTrace();
            //System.exit(1);

        }
        if(keyAndEdges.length>1){
            value = keyAndEdges[edgesPosition]; // the list of adjacent nodes, distance, color, parent
            String[] tokens = value.split(" "); // split the value into tokens where
            //tokens[0] = list of adjacent nodes, tokens[1]= distance, tokens[2]= color, tokens[3]= parent

            // setting the edges of the node
            for (String s : tokens) {
                if (s.length() > 0) {
                    edges.add(s);
                }
            }
        }
        if(inputLine.length>distancePosition){ // Distance

            if (inputLine[distancePosition].trim().equals(MAX_VALUE)) {
                this.distance = Integer.MAX_VALUE;
            } else {

                this.distance = Integer.parseInt(inputLine[1].trim());
            }
        }
        else{
            this.distance = Integer.MAX_VALUE;
        }
        if(inputLine.length>colorPosition){ // Color
            this.color = Color.valueOf(inputLine[colorPosition]);
        }
        else{
            this.color = Color.WHITE;
        }

        if(inputLine.length>parentPosition){ // Parent
            this.parent = inputLine[parentPosition].trim();
        }
        else{
            this.parent = NULL;
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
            s.append(MAX_VALUE);
        }
        s.append("|");
        s.append(color.toString());
        s.append("|");
        s.append(parent);


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
    public Color getColor() {
        return this.color;
    }

    public void setColor(Color color) {
        this.color = color;
    }
    public void setParent(String parent) {
        this.parent = parent;
    }

    public String getParent() {
        return parent;
    }

}