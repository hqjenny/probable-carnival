package com.databricks.apps.twitter_classifier

import com.google.gson.{GsonBuilder, JsonParser}
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.clustering.KMeansModel
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
 * OutputClusters
 */
object OutputClusters {
  val jsonParser = new JsonParser()
  val gson = new GsonBuilder().setPrettyPrinting().create()
 
  def main(args: Array[String]) {
    // Process program arguments and set properties
    if (args.length < 2) {
      System.err.println("Usage: " + this.getClass.getSimpleName +
        " <tweetInput> <outputModelDir> <numClusters> <numIterations>")
      System.exit(1)
    }
    val Array(tweetInput, outputModelDir) = args

    val conf = new SparkConf().setAppName(this.getClass.getSimpleName)
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    // Pretty print some of the tweets.
    //println("------------Sample JSON Tweets-------")
    //for (tweet <- tweets.take(5)) {
    //  println(gson.toJson(jsonParser.parse(tweet)))
    //}

    val tweetTable = sqlContext.read.json(tweetInput).cache()
    tweetTable.registerTempTable("tweetTable")

    //println("------Tweet table Schema---")
    //tweetTable.printSchema()

    //println("----Sample Tweet Text-----")
    //sqlContext.sql("SELECT text FROM tweetTable LIMIT 10").collect().foreach(println)

    //println("------Sample Lang, Name, text---")
    //sqlContext.sql("SELECT lang, user.lang, user.name, text FROM tweetTable LIMIT 5").collect().foreach(println)

    //println("------Total count by languages Lang, count(*)---")
    //sqlContext.sql("SELECT user.lang, COUNT(*) as cnt FROM tweetTable GROUP BY user.lang ORDER BY cnt DESC LIMIT 25").collect.foreach(println)

    val texts = sqlContext.sql("SELECT 0,id, lang, user.lang, text,0 from tweetTable").map(_.toString)
    //val model = KMeans.train(vectors, numClusters, numIterations)
    val model = new KMeansModel(sc.objectFile[Vector](outputModelDir.toString).collect())
    //val num_entries = texts.count()
    val all_texts = texts.collect()
      all_texts.foreach { t => {
        val splits = t.split(",")
        val len = splits.length
        val text = splits.slice(4,len-1)
        val model_index = model.predict(Utils.featurize(text.mkString(",")))
          println(splits(2)+" " + splits(3) +" "+model_index)
        }
      }
  }
}
