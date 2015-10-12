import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by hastimal on 10/11/2015.
 */
object GeneralPracticeSpark {
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir","F:\\winutils")
    // initialise spark context
    val conf = new SparkConf().setAppName("WordCountSpark").setMaster("local[2]").set("spark.executor.memory","4g")
    val sc = new SparkContext(conf)
    val list = sc.textFile("")
    // do stuff
/***/

    //done job
    println("Successfully done!!")
    sc.stop()
  }
}
