import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.ml.feature.{StopWordsRemover, Tokenizer, Word2Vec}
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.evaluation.ClusteringEvaluator

object Main {
  def main(args: Array[String]) {
    val spark = SparkSession.builder
      .appName("Simple Application")
      .master("local[*]")
      .getOrCreate()
    import spark.implicits._


    // write your code here


    spark.stop()
  }
}
