import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.feature.HashingTF
import java.io._

val numFeatures = 1000
val tf = new HashingTF(numFeatures)
def featurize(s: String): Vector = {
    tf.transform(s.sliding(2).toSeq)
}
def writeFormat(t: (Vector, Long)): String = {

    t._2 + " " + (t._1.toArray mkString " ")
}

val tweetInput = "hdfs:///tmp/tweets/tweets*/part-*"
val tweets = sc.textFile(tweetInput)
val tweetTable = sqlContext.read.json(tweetInput).cache()
tweetTable.registerTempTable("tweetTable")
val texts = sqlContext.sql("SELECT text from tweetTable").map(_.toString)
val vectorsRDD = texts.map(featurize).zipWithIndex.map(writeFormat)
val vectors = vectorsRDD.collect()

val file = "input-tweets.txt"
val writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file)))
for (x <- vectors) {
  writer.write(x + "\n")  // however you want to format it
}
writer.close()