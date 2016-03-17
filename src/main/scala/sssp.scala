package src.main.scala

import org.apache.spark.graphx._
// Import random graph generation library
// A graph with edge attributes containing distances
import org.apache.spark.{SparkConf, SparkContext}
object SimpleSSSP {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Simple Application")
    val sc = new SparkContext(conf)
    //args(0) = "hdfs://localhost:9000/user/gaston/ssspspark"
    val graph = GraphLoader.edgeListFile(sc, args(0), false, 1).mapEdges(e => e.attr.toDouble)
    //GraphGenerators.logNormalGraph(sc, numVertices = 10).mapEdges(e => e.attr.toDouble)
    val sourceId: VertexId =   args(1).toLong// The ultimate source
    // Initialize the graph such that all vertices except the root have distance infinity.
    val initialGraph = graph.mapVertices((id, _) => if (id == sourceId) 0.0 else Double.PositiveInfinity)
    initialGraph.cache()
    val sssp = initialGraph.pregel(Double.PositiveInfinity)(
      (id, dist, newDist) => math.min(dist, newDist), // Vertex Program
      triplet => {
        // Send Message
        if (triplet.srcAttr + triplet.attr < triplet.dstAttr) {
          Iterator((triplet.dstId, triplet.srcAttr + triplet.attr))
        } else {
          Iterator.empty
        }
      },
      (a, b) => math.min(a, b) // Merge Message
    )
    println(sssp.vertices.collect.mkString("\n"))
  }
}
