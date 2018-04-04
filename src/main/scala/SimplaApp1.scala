/* SimpleApp.scala */
import org.apache.spark.sql.SparkSession
import java.io.File
import java.io.PrintWriter

object SimplaApp1 {
  def main(args: Array[String]) {
    val logFile = "YOUR_SPARK_HOME/README.md" // Should be some file on your system
    val spark = SparkSession.builder.appName("Simple Application").getOrCreate()
    val logData = spark.read.textFile(logFile).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()

    val logLine = spark.read.textFile(logFile).rdd
    println(s"******SYSOUT**************")
    logData.take(3).foreach(println)
    logLine.take(3).foreach(println)

    println(s"Lines with a: $numAs, Lines with b: $numBs")

    val sc = spark.sparkContext
      val filereadme = sc.textFile {
        "file:///home/vijay/Downloads/spark-2.2.0-bin-hadoop2.7/README.md"
      }

    filereadme.cache()

    filereadme.take(3).foreach(println)


    val cnt1 = filereadme.count()

    println(s"Lines with c: $cnt1")

    val wordCount = filereadme.flatMap(line => line.split(" ")).map(word => (word,1)).reduceByKey(_+_)

    println(s"*************************")
    wordCount.take(3).foreach(println)

    println(s"*************************")

    wordCount.saveAsTextFile("file:///tmp/cnt/")

    val writer = new PrintWriter(new File("/tmp/numAs.txt"))

    writer.write("Lines with a:%s".format(String.valueOf(numAs)))
    writer.close()
    spark.stop()
  }
}