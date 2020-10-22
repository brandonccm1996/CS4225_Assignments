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


    val means   = kmeans(sampleVectors(vectors), vectors, 1, debug = true)
    val results = clusterResults(means, vectors)
    printResults(results)
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
  /** Eg of 1 line in input: Posting(1,27233496,None,0,Some(Data-Analysis)) */
  /** Eg of 1 line in input: Posting(2,5494879,Some(5484340),1,None) */
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
  /** Eg of 1 line in input: (11335234,CompactBuffer((Posting(1,11335234,None,2,Some(Data-Analysis)),Posting(2,11335253,Some(11335234),3,None)), (Posting(1,11335234,None,2,Some(Data-Analysis)),Posting(2,11335254,Some(11335234),0,None)))) */
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
  /** Eg of 1 line in input: (Posting(1,21090507,None,1,Some(Silicon Valley)),6) */
  def vectorPostings(scoredPosts: RDD[(Posting, Int)]): RDD[(Int, Int)] = {

//    println("scoredPosts")
//    scoredPosts.take(10).foreach(println)
//    scoredPosts.take(10).foreach(scoredPost => println(Domains.indexOf(scoredPost._1.tags.get)))

    // extract just the fields needed into local variables to prevent passing all of this
    val domains_ = this.Domains
    val domainSpread_ = this.DomainSpread

    // Return (D*X, S) after removing qns with no tags
    scoredPosts
      .filter(_._1.tags != None)
      .map {
        case (qnPosting, maxAnsScore) => {
          val indexInDomainList = domains_.indexOf(qnPosting.tags.get)
          (indexInDomainList * domainSpread_, maxAnsScore)
        }
      }
  }


  /** Select k random points as initial centroids */
  /** Eg of 1 line in input: (300000,6) */
  def sampleVectors(vectors: RDD[(Int, Int)]): Array[(Int, Int)] = {

//    println("sampleVectors")
//    vectors.take(10).foreach(println)

    vectors.takeSample(true, kmeansKernels)
  }


  //
  //
  //  Kmeans method:
  //
  //

  /** Main kmeans computation */
  @tailrec final def kmeans(clusterCentroids: Array[(Int, Int)], vectors: RDD[(Int, Int)], iterationCount: Int, debug: Boolean): Array[(Int, Int)] = {

//    println("kMeans")

    // extract just the field needed into a local variable to prevent passing all of this
    val kmeansMaxIterations_ = this.kmeansMaxIterations

    // return every centroid with the vectors in its cluster
    val centroidForGroupedVectors = obtainCentroidWithGroupedVectors(clusterCentroids, vectors)

    // find new centroid locations based on their grouped vectors
    val oldAndNewCentroids = centroidForGroupedVectors.map {
      case(centroid, vector) => {
        val newCentroid = averageVectors(vector)
        (centroid, newCentroid)
      }
    }.collect()

    // get new cluster centroids
    val newClusterCentroids = clusterCentroids map(identity)
    oldAndNewCentroids.foreach {
      case(oldCentroid, newCentroid) => {
        newClusterCentroids.update(oldCentroid, newCentroid)
      }
    }

    val distance = euclideanDistance(clusterCentroids, newClusterCentroids)
    val hasConverged = converged(distance)

    // repeat kmeans iteration until converged or function has iterated kmeansMaxIterations_ times
    if (hasConverged || iterationCount > kmeansMaxIterations_) {
      newClusterCentroids
    } else {
      kmeans(newClusterCentroids, vectors, iterationCount+1, debug = true)
    }
  }

  def clusterResults(clusterCentroids: Array[(Int, Int)], vectors: RDD[(Int, Int)]): Array[(String, Int, Int, Int)] = {

//    println("clusterResults")

    // extract just the fields needed into local variables to prevent passing all of this
    val domains_ = this.Domains
    val domainSpread_ = this.DomainSpread

    // return every centroid with the vectors in its cluster
    val centroidForGroupedVectors = obtainCentroidWithGroupedVectors(clusterCentroids, vectors)

//    println(centroidForGroupedVectors)
//    centroidForGroupedVectors.foreach {
//      case (centroid, groupedVectors) => {
//        println(centroid)
//        println(groupedVectors)
//      }
//    }

    val result = centroidForGroupedVectors.map {
      case (centroid, groupedVectors) => {
        val arrayOfDomainIdMultipledWithD = groupedVectors.map(groupedVector => groupedVector._1).toArray
        val domainId = arrayOfDomainIdMultipledWithD(0) / domainSpread_
        val domainName = domains_(domainId)
        val clusterSize = groupedVectors.size
        val medianScore = computeMedian(groupedVectors)
        val avgScore = computeAverage(groupedVectors)
        (domainName, clusterSize, medianScore, avgScore)
      }
    }

    // collect the entire results RDD and sort by avgScore for easier gathering of insights
//    result.collect().take(10).foreach(println)
    result.collect().sortBy {
      case (domainName, clusterSize, medianScore, avgScore) => {
        avgScore
      }
    }
  }


  //
  //
  //  Kmeans utilities (Just some cases, you can implement your own utilities.)
  //
  //

  /** Return every centroid with the vectors in its cluster */
  /** Eg of 1 line in output:  (19, CompactBuffer((0,575), (0,231), (0,258), (0,234))) */
  def obtainCentroidWithGroupedVectors(clusterCentroids: Array[(Int, Int)], vectors: RDD[(Int, Int)]): RDD[(Int, Iterable[(Int, Int)])] = {
    // calculate closest pt ie centroid for each vector
    val centroidForEachVector = vectors.map(vector => {
      val closestPt = findClosest(vector, clusterCentroids)
      (closestPt, vector)
    })

    // return every centroid with the vectors in its cluster
    val centroidForGroupedVectors = centroidForEachVector.groupByKey()
    centroidForGroupedVectors
  }


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

  def computeAverage(a: Iterable[(Int, Int)]) = {
    val s = a.map(x => x._2).toArray
    var sum = 0
    s.foreach(score => {
      sum = sum + score
    })
    sum/s.length
  }


  //  Displaying results:

  def printResults(results: Array[(String, Int, Int, Int)]): Unit  = {
    println("Cluster centroid | Domain Size | Median Score | Average Score")
    results.foreach {
      case(centroid, domainSize, medianScore, avgScore) => {
        println(centroid + " | " + domainSize + " | " + medianScore + " | " + avgScore)
      }
    }
  }
}
