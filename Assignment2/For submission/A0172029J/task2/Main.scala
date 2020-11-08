import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.ml.feature.{StopWordsRemover, Tokenizer, Word2Vec}
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.evaluation.ClusteringEvaluator

object Main {
  def main(args: Array[String]) {
    val spark = SparkSession.builder
      .appName("CS4225 K Means Task 2")
      .master("local[*]")
      .getOrCreate()
    import spark.implicits._


    // write your code here

    // Remove stopwords
    val stopWordLines = spark.read.text(args(1))
    val stopWordLinesArray = stopWordLines.collect.map(row=>row.getString(0))

    val tweetsDataFrame = spark.read.format("csv").load(args(0)).select("_c5")
//    tweetsDataFrame.show(2, false)

    val tokenizer = new Tokenizer().setInputCol("_c5").setOutputCol("tweetsWordsRaw")
    val tokenized: DataFrame = tokenizer.transform(tweetsDataFrame)

    val remover = new StopWordsRemover().setInputCol("tweetsWordsRaw").setOutputCol("tweetsWordsFiltered").setStopWords(stopWordLinesArray)
    val filtered: DataFrame = remover.transform(tokenized).select("tweetsWordsFiltered")
//    filtered.show(2, false)

    // Convert to word2vec format
    val word2vec = new Word2Vec().setInputCol("tweetsWordsFiltered").setOutputCol("word2vec_result")
    val word2vec_model = word2vec.fit(filtered)
    val word2vecDataframe = word2vec_model.transform(filtered).select("word2vec_result")
//    word2vecDataframe.show(2, false)
    word2vecDataframe.cache()

    // Train a k-means model
    val kmeans = new KMeans().setMaxIter(20).setK(2).setFeaturesCol("word2vec_result").setPredictionCol("predicted_cluster")
    val kmeans_model = kmeans.fit(word2vecDataframe)
    val dataframeClustered = kmeans_model.transform(word2vecDataframe)
//    dataframeClustered.show(2, false)

    // Evaluate clustering by computing Silhouette score
    val evaluator = new ClusteringEvaluator().setFeaturesCol("word2vec_result").setPredictionCol("predicted_cluster")

    val silhouette = evaluator.evaluate(dataframeClustered)
    println(s"Silhouette with squared euclidean distance = $silhouette")

    // Print cluster centers result
    println("Number of Cluster Centers: ")
    println(kmeans_model.clusterCenters.length)
    println("MaxIter:")
    println(kmeans.getMaxIter)
    println("Cluster Centers: ")
    kmeans_model.clusterCenters.foreach(println)

    spark.stop()
  }
}
