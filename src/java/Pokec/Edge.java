package Pokec;

/**
 * Created by gestol on 11/11/15.
 */
public class Edge {

    Node fromNode;
    Node toNode;
    int weight;

    public Edge(String edgeInfo){
        String[] inputLine = edgeInfo.split("\t"); //splitting the input line record by tab delimiter into key and value
        try{
            fromNode = new Node(inputLine[0]);
            fromNode.addEdge(inputLine[0]);
            toNode = new Node(inputLine[1]);
            // weight =  Integer.valueOf(inputLine[2])
            weight = 1;
        }catch (Exception e) {
            e.printStackTrace();
            System.exit(1);

        }

    }

}
