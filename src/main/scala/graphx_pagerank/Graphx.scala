package graphx_pagerank
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

object Graphx extends App {
  val vertexArray = Array(
    (1L, ("Alice", 28)),
    (2L, ("Bob", 27)),
    (3L, ("Charlie", 65)),
    (4L, ("David", 42)),
    (5L, ("Ed", 55)),
    (6L, ("Fran", 50))
  )
  val edgeArray = Array(
    Edge(2L, 1L, 7),
    Edge(2L, 4L, 2),
    Edge(3L, 2L, 4),
    Edge(3L, 6L, 3),
    Edge(4L, 1L, 1),
    Edge(5L, 2L, 2),
    Edge(5L, 3L, 8),
    Edge(5L, 6L, 3)
  )

  val conf: SparkConf = new SparkConf()
    .setMaster("local[*]")
    .setAppName("graphx")
    .set("spark.driver.host", "localhost")
  val sc: SparkContext = new SparkContext(conf)

  val vertexRDD: RDD[(Long, (String, Int))] = sc.parallelize(vertexArray)
  val edgeRDD: RDD[Edge[Int]] = sc.parallelize(edgeArray)
  /*User Name and Age and the edge property - number of Likes*/
  val graph: Graph[(String, Int), Int] = Graph(vertexRDD, edgeRDD)

  /*name of people over 30*/
  graph.vertices.filter {
    case (id, (name, age)) => age > 30
  }.collect.foreach {
    case (id, (name, age)) => println(s"$name is $age")
  }

  /*who likes whom*/
  for (triplet <- graph.triplets.collect) {
    /**
     * Triplet has the following Fields:
     *   triplet.srcAttr: (String, Int) // triplet.srcAttr._1 is the name
     *   triplet.dstAttr: (String, Int)
     *   triplet.attr: Int
     *   triplet.srcId: VertexId
     *   triplet.dstId: VertexId
     */

    println(
      s"${triplet.srcAttr._1} " +
      {if (triplet.attr > 5) "loves " else "likes "} +
      s"${triplet.dstAttr._1}")
  }

}
