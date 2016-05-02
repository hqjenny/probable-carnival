object FormatTweetsForCUDA {
    import org.apache.spark.mllib.linalg.Vector
    import org.apache.spark.mllib.feature.HashingTF
    import java.io._
    import org.apache.spark.SparkContext
    import org.apache.spark.SparkContext._
    import org.apache.spark.SparkConf
    import org.apache.spark.rdd._
    import org.apache.spark.sql.SQLContext
    import org.apache.spark.sql._
    import org.apache.spark.sql.SQLContext._
    def main(args: Array[String]) {
        val conf = new SparkConf().setAppName("FormatTweetsForCUDA")
        val sc = new SparkContext(conf)
        val sqlContext= new SQLContext(sc)

        import sqlContext.implicits._ 
        if (args.length < 2) {
            System.err.println("Usage: " + this.getClass.getSimpleName +
                "<input_file> <output_file>")
            System.exit(1)
        }

        val numFeatures = 1000
        val tf = new HashingTF(numFeatures)
        val tweetInput = args(0)

        val tweets = sc.textFile(tweetInput)
        val tweetTable = sqlContext.read.json(tweetInput).cache()
        tweetTable.registerTempTable("tweetTable")
        val texts = sqlContext.sql("SELECT text from tweetTable").map(_.toString)
        val vectorsRDD = texts.map(featurize(tf)).zipWithIndex.map(writeFormat)
        //val vectors = vectorsRDD.collect()

        val file = args(1)
        vectorsRDD.saveAsTextFile(file)
    }

    def featurize(tf: HashingTF)( s: String): Vector = {
        tf.transform(s.sliding(2).toSeq)
    }

    def writeFormat(t: (Vector, Long)): String = {
        t._2 + " " + (t._1.toArray mkString " ")
    }
}
