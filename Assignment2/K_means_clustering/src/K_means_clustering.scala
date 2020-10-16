import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.PairRDDFunctions
import annotation.tailrec
import scala.reflect.ClassTag

/** A raw posting, either a question or an answer */
case class Posting(postingType: Int, id: Int, parentId: Option[Int], score: Int, tags: Option[String]) extends Serializable

/** The main class */
object Assignment2 extends Assignment2 {

  @transient lazy val conf: SparkConf = new SparkConf().setMaster("local").setAppName("Assignment2")
  @transient lazy val sc: SparkContext = new SparkContext(conf)

  //sc.setLogLevel("WARN")

  /** Main function */
  def main(args: Array[String]): Unit = {

    val lines   = sc.textFile("QA_data.csv")
    val raw     = rawPostings(lines)
    val grouped = groupedPostings(raw)
    val scored  = scoredPostings(grouped)
    val vectors = vectorPostings(scored)
//
//
//    val means   = kmeans(sampleVectors(vectors), vectors, debug = true)
//    val results = clusterResults(means, vectors)
//    printResults(results)
  }
}


/** The parsing and kmeans methods */
class Assignment2 extends Serializable {

  /** Languages */
  val Domains =
    List(
      "Machine-Learning", "Compute-Science", "Algorithm", "Big-Data", "Data-Analysis", "Security", "Silicon Valley", "Computer-Systems",
      "Deep-learning", "Internet-Service-Providers", "Programming-Language", "Cloud-services", "Software-Engineering", "Embedded-System", "Architecture")


  /** K-means parameter: How "far apart" languages should be for the kmeans algorithm? */
  def DomainSpread = 50000
  assert(DomainSpread > 0)

  /** K-means parameter: Number of clusters */
  def kmeansKernels = 45

  /** K-means parameter: Convergence criteria, if changes of all centriods < kmeansEta, stop*/
  def kmeansEta: Double = 20.0D

  /** K-means parameter: Maximum iterations */
  def kmeansMaxIterations = 120


  //
  //
  // Parsing utilities:
  //
  //

  /** Load postings from the given file */
  def rawPostings(lines: RDD[String]): RDD[Posting] =
    lines.map(line => {
      val arr = line.split(",")
      Posting(postingType =    arr(0).toInt,
        id =             arr(1).toInt,
        parentId =       if (arr(2) == "") None else Some(arr(2).toInt),
        score =          arr(3).toInt,
        tags =           if (arr.length >= 5) Some(arr(4).intern()) else None)
    })


  /** Group the questions and answers together */
  def groupedPostings(rawPosts: RDD[Posting]): RDD[(Int, Iterable[(Posting, Posting)])] = {
    // Filter the questions and answers separately
    // Prepare them for a join operation by extracting the QID value in the first element of a tuple.

//    println("rawPosts")
//    rawPosts.take(10).foreach(println)

    val qns = rawPosts
      .filter(_.postingType == 1)
      .map(rawPostQn => (rawPostQn.id, rawPostQn))

    val ans = rawPosts
      .filter(_.postingType == 2)
      .map(rawPostAns => (rawPostAns.parentId.get, rawPostAns))

    // join qns and ans with same id (for qns) or parentId (for ans)
    qns.join(ans).groupByKey()
  }


  /** Compute the maximum score for each posting */
  def scoredPostings(groupedPosts: RDD[(Int, Iterable[(Posting, Posting)])]): RDD[(Posting, Int)]  = {
//    println("groupedPosts")
//    groupedPosts.take(10).foreach(println)
//    groupedPosts.take(10).foreach(groupedPost => {
//      println(groupedPost._1)
//      println(groupedPost._2)
//    })

    groupedPosts.map {
      case(id, qnAndAnsPosts) => {
        var maxScore = 0

        val qnsPostsArr = qnAndAnsPosts.map(qnAndAnsPost => qnAndAnsPost._1).toArray
        val ansPostsArr = qnAndAnsPosts.map(qnAndAnsPost => qnAndAnsPost._2).toArray
        for (x <- 0 to ansPostsArr.size-1) {
          if (ansPostsArr(x).score  > maxScore) {
            maxScore = ansPostsArr(x).score
          }
        }

        // Return the qnPosting and the maxScore of all the ansPostings for that qnPosting
        (qnsPostsArr(0), maxScore)
      }
    }
  }


  /** Compute the vectors for the kmeans */
  def vectorPostings(scoredPosts: RDD[(Posting, Int)]): RDD[(Int, Int)] = {
//    println("scoredPosts")
//    scoredPosts.take(10).foreach(println)
//    scoredPosts.take(10).foreach(scoredPost => println(Domains.indexOf(scoredPost._1.tags.get)))

    // Return (D*X, S)
    scoredPosts
      .filter(_._1.tags != None)
      .map(scoredPost => (Domains.indexOf(scoredPost._1.tags.get) * DomainSpread, scoredPost._2))
  }




  //
  //
  //  Kmeans method:
  //
  //

  /** Main kmeans computation */
//  @tailrec final def kmeans(): = {
//
//    //ToDo
//
//  }


  //
  //
  //  Kmeans utilities (Just some cases, you can implement your own utilities.)
  //
  //

  /** Decide whether the kmeans clustering converged */
  def converged(distance: Double) = distance < kmeansEta


  /** Return the euclidean distance between two points */
  def euclideanDistance(v1: (Int, Int), v2: (Int, Int)): Double = {
    val part1 = (v1._1 - v2._1).toDouble * (v1._1 - v2._1)
    val part2 = (v1._2 - v2._2).toDouble * (v1._2 - v2._2)
    part1 + part2
  }

  /** Return the euclidean distance between two points */
  def euclideanDistance(a1: Array[(Int, Int)], a2: Array[(Int, Int)]): Double = {
    assert(a1.length == a2.length)
    var sum = 0d
    var idx = 0
    while(idx < a1.length) {
      sum += euclideanDistance(a1(idx), a2(idx))
      idx += 1
    }
    sum
  }

  /** Return the closest point */
  def findClosest(p: (Int, Int), centers: Array[(Int, Int)]): Int = {
    var bestIndex = 0
    var closest = Double.PositiveInfinity
    for (i <- 0 until centers.length) {
      val tempDist = euclideanDistance(p, centers(i))
      if (tempDist < closest) {
        closest = tempDist
        bestIndex = i
      }
    }
    bestIndex
  }


  /** Average the vectors */
  def averageVectors(ps: Iterable[(Int, Int)]): (Int, Int) = {
    val iter = ps.iterator
    var count = 0
    var comp1: Long = 0
    var comp2: Long = 0
    while (iter.hasNext) {
      val item = iter.next
      comp1 += item._1
      comp2 += item._2
      count += 1
    }
    ((comp1 / count).toInt, (comp2 / count).toInt)
  }


  def computeMedian(a: Iterable[(Int, Int)]) = {
    val s = a.map(x => x._2).toArray
    val length = s.length
    val (lower, upper) = s.sortWith(_<_).splitAt(length / 2)
    if (length % 2 == 0) (lower.last + upper.head) / 2 else upper.head
  }

  //  Displaying results:

//  def printResults(results: ):  = {
//
//  }
}
