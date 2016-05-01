#!/bin/sh
exec scala "$0" "$@"
!#

object FormatTweetsForCUDA {
    import org.apache.spark.mllib.linalg.Vector
    import org.apache.spark.mllib.feature.HashingTF
    import java.io._

    def main(args: Array[String]) {
        if (args.length < 2) {
            System.err.println("Usage: " + this.getClass.getSimpleName +
                "<input_file> <output_file>")
            System.exit(1)
        }

        val numFeatures = 1000
        val tf = new HashingTF(numFeatures)
        val tweetInput = "hdfs://" + args(0)

        val tweets = sc.textFile(tweetInput)
        val tweetTable = sqlContext.read.json(tweetInput).cache()
        tweetTable.registerTempTable("tweetTable")
        val texts = sqlContext.sql("SELECT text from tweetTable").map(_.toString)
        val vectorsRDD = texts.map(featurize).zipWithIndex.map(writeFormat)
        val vectors = vectorsRDD.collect()

        val file = args(1)
        val writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file)))
        for (x <- vectors) {
          writer.write(x + "\n")
        }
        writer.close()
    }

    def featurize(s: String): Vector = {
        tf.transform(s.sliding(2).toSeq)
    }

    def writeFormat(t: (Vector, Long)): String = {
        t._2 + " " + (t._1.toArray mkString " ")
    }
}

FormatTweetsForCUDA.main(args)