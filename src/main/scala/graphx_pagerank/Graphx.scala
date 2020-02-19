package graphx_pagerank
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

object Graphx extends App {
  private def namePeopleOver30() = {
    /*name of people over 30*/
    graph.vertices.filter { case (id, (name, age)) => age > 30 }.collect.foreach {
      case (id, (name, age)) => println(s"$name is $age")
    }
  }
  private def whoLikesWhom() = {
    /*who likes whom*/
    for (triplet <- graph.triplets.collect) {
      println(s"${triplet.srcAttr._1} " + {
        if (triplet.attr > 5) "loves " else "likes "
      } + " ${triplet.dstAttr._1}")
    }
  }
  private def printNameAndLilkedByWhom = {
    /*User 1 is called Alice and is liked by 2 people.*/
    userGraph.vertices.foreach { v =>
      println(s"User ${v._1} is called ${v._2.name} and is liked by ${v._2.inDeg} people")
    }
  }
  private def usersWhoAreLikeByTheSameNumberOfPoepleTheyLike = {
    //  Print the names of the users who are liked by the same number of people they like.
    userGraph.vertices.filter {
      case (id, u) => u.inDeg == u.outDeg
    }.collect.foreach {
      case (id, property) => println(property.name)
    }
  }

  val vertexArray = Array(
    (1L, ("Alice", 28)),
    (2L, ("Bob", 27)),
    (3L, ("Charlie", 65)),
    (4L, ("David", 42)),
    (5L, ("Ed", 55)),
    (6L, ("Fran", 50))
  )
  /*srcId, dstId, attr*/
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

  /*id, (name, age)*/
  val vertexRDD: RDD[(VertexId, (String, PartitionID))] = sc.parallelize(vertexArray)
  /*srcId, dstId, attr = likes*/
  val edgeRDD: RDD[Edge[PartitionID]] = sc.parallelize(edgeArray)
  /*User Name and Age and the edge property - number of Likes*/
  val graph: Graph[(String, PartitionID), PartitionID] = Graph(vertexRDD, edgeRDD)

//  namePeopleOver30()
//  whoLikesWhom()

  // Define a class to more clearly model the user property
  case class User(name: String, age: PartitionID, inDeg: PartitionID, outDeg: PartitionID)
  // Create a user Graph
  /*triplet: ((2,User(Bob,27,0,0)),(1,User(Alice,28,0,0)),7)*/
  val initialUserGraph: Graph[User, PartitionID] = graph.mapVertices{ case (id, (name, age)) => User(name, age, 0, 0) }

  // Fill in the degree information
//  ug: ((2,User(Bob,27,2,2)),(1,User(Alice,28,2,0)),7)
  val userGraph = initialUserGraph.outerJoinVertices(initialUserGraph.inDegrees) {
    case (id, u, inDegOpt) => User(u.name, u.age, inDegOpt.getOrElse(0), u.outDeg)
  }.outerJoinVertices(initialUserGraph.outDegrees) {
    case (id, u, outDegOpt) => User(u.name, u.age, u.inDeg, outDegOpt.getOrElse(0))
  }

//  printNameAndLilkedByWhom
//  usersWhoAreLikeByTheSameNumberOfPoepleTheyLike


  // Find the oldest follower for each user
  /*olderst follower: (1,(David,42))*/
  val oldestFollower: VertexRDD[(String, PartitionID)] = userGraph.aggregateMessages[(String, PartitionID)](
    // For each edge send a message to the destination vertex with the attribute of the source vertex
    sendMsg = { triplet => triplet.sendToDst(triplet.srcAttr.name, triplet.srcAttr.age) },
    // To combine messages take the message for the older follower
    mergeMsg = {(a, b) => if (a._2 > b._2) a else b}
  )

  println("olf:"+oldestFollower.count())//empty
  for (of <- oldestFollower) {
    println("olderst follower: " + of)
  }

  userGraph.vertices.leftJoin(oldestFollower) { (id, user, optOldestFollower) =>
    optOldestFollower match {
      case None => s"${user.name} does not have any followers."
      case Some((name, age)) => s"${name} is the oldest follower of ${user.name}."
    }
  }.collect.foreach {
    case (id, str) => println(str)
  }


}
