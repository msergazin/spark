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

  val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("graphx").set("spark.driver.host", "localhost")
  val sc: SparkContext = new SparkContext(conf)

  val vertexRDD: RDD[(Long, (String, Int))] = sc.parallelize(vertexArray)
  val edgeRDD: RDD[Edge[Int]] = sc.parallelize(edgeArray)
  /*User Name and Age and the edge property - number of Likes*/
  val graph: Graph[(String, Int), Int] = Graph(vertexRDD, edgeRDD)

  /*name of people over 30*/
  graph.vertices.filter {case (id, (name, age)) => age > 30}.collect.foreach {
    case (id, (name, age)) => println(s"$name is $age")
  }

  /*who likes whom*/
  for (triplet <- graph.triplets.collect) {
    println(s"${triplet.srcAttr._1} " + {if (triplet.attr > 5) "loves " else "likes "} +" ${triplet.dstAttr._1}")
  }

  // Define a class to more clearly model the user property
  case class User(name: String, age: Int, inDeg: Int, outDeg: Int)
  // Create a user Graph
  val initialUserGraph: Graph[User, Int] = graph.mapVertices{ case (id, (name, age)) => User(name, age, 0, 0) }


  // Fill in the degree information
  val userGraph = initialUserGraph.outerJoinVertices(initialUserGraph.inDegrees) {
    case (id, u, inDegOpt) => User(u.name, u.age, inDegOpt.getOrElse(0), u.outDeg)
  }.outerJoinVertices(initialUserGraph.outDegrees) {
    case (id, u, outDegOpt) => User(u.name, u.age, u.inDeg, outDegOpt.getOrElse(0))
  }

  /*User 1 is called Alice and is liked by 2 people.*/
  userGraph.vertices.foreach{ v =>
    println(s"User ${v._1} is called ${v._2.name} and is liked by ${v._2.inDeg} people")
  }

//  Print the names of the users who are liked by the same number of people they like.
  userGraph.vertices.filter {
    case (id, u) => u.inDeg == u.outDeg
  }.collect.foreach {
    case (id, property) => println(property.name)
  }

  // Find the oldest follower for each user
  val oldestFollower: VertexRDD[(String, Int)] = userGraph.aggregateMessages[(String, Int)](
    // For each edge send a message to the destination vertex with the attribute of the source vertex
    edge => Iterator((edge.dstId, (edge.srcAttr.name, edge.srcAttr.age))),
    // To combine messages take the message for the older follower
    (a, b) => if (a._2 > b._2) a else b
  )

  userGraph.vertices.leftJoin(oldestFollower) { (id, user, optOldestFollower) =>
    /**
     * Implement: Generate a string naming the oldest follower of each user
     * Note: Some users may have no messages optOldestFollower.isEmpty if they have no followers
     *
     * Try using the match syntax:
     *
     *  optOldestFollower match {
     *    case None => "No followers! implement me!"
     *    case Some((name, age)) => "implement me!"
     *  }
     *
     */
    optOldestFollower match {
      case None => "no followers!"
      case Some((name, age)) => name
    }
  }.collect.foreach {
    case (id, str) => println(str)
  }
}
