/**
 * Created by hastimal on 10/11/2015.
 */
import org.apache.spark.{SparkConf, SparkContext}

import scala.reflect.io.Path

/**
 * Created by hastimal on 10/11/2015.
 */
object WordCountSpark {
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir","F:\\winutils")
    // initialise spark context
    val conf = new SparkConf().setAppName("WordCountSpark").setMaster("local[2]").set("spark.executor.memory","4g")
    val sc = new SparkContext(conf)

    // do stuff

    println("Hello, See below WC !")
    // Get the text file from filesystem
    val textFile = sc.textFile("src/main/resources/inputFile/sampleWordCountInput.txt")
    val counts = textFile.flatMap(line => line.split(" "))
      .map(word => (word, 1)).reduceByKey((x,y)=>x+y)
//      .reduceByKey(_+_)
    //counts.saveAsTextFile("src/main/resources/outFiles/sampleWC.txt")
    //Deleting output files recursively if exists
    val dir = Path("src/main/resources/outputFiles")
    if (dir.exists) {
      dir.deleteRecursively()
      println("Successfully existing output deleted!!")
    }
    println("Writing in new files as output.......")
    counts.saveAsTextFile("src/main/resources/outputFiles/sampleWordCountOutPut.txt")
    counts.foreach(i=>println(i))
    println("Successfully done!!")
    //Stopping spark
    sc.stop()

   // sc.awaitTermination()
  }
}
