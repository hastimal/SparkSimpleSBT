import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by hastimal on 10/11/2015.
 */
object GeneralSpark {
  def main(args: Array[String]) {
    //Processing collections with functional programming
    //
    val conf = new SparkConf().setAppName("WordCountSpark").setMaster("local[2]").set("spark.executor.memory","4g")
    val sc = new SparkContext(conf)

    val nums= sc.parallelize(List(1,2,3,4,5))
  //Retrieve RDD as local collection
    println(nums.collect().toList)  //List(1,2,3,4,5) //
    println(nums.take(2).toList)//Take List(1,2) //Return first K elements
    println(nums.count())//3 //Count number of elements
    println(nums.reduce{case(x,y)=>x+y})//3 //Merge elements with an associative function
    //nums.saveAsObjectFile("hdfs://file.txt")
    val squares = nums.map(x=>x*x)  //Make same for sqqaures
    val even = squares.filter(x=>x%2==0)  //Keep elements passing a predicate
    println("Even: "+even.collect().toList)

    //SOme key-value pair and operations
    val pets= sc.parallelize(List(("cat",1),("dog",2),("cat",5)))
    println("after reduce by Key "+pets.reduceByKey(_+_).collect().toList)// List((dog,2), (cat,6))
    println("after reduce by Key "+pets.reduceByKey(_+_).collect().toSeq)//WrappedArray((dog,2), (cat,6))
    println("after group by Key "+pets.groupByKey().collect().toList)//Key List((dog,CompactBuffer(2)), (cat,CompactBuffer(1, 5)))
    println("after sort by Key "+pets.sortByKey().collect().toList)

    //Reduceby Key automatically implement combiner on map side
    println("###List transversed #### :")
    val list = List(1,2,3)
    list.foreach(x=>print(" "+x))

    println("###Map transversed #### :")
    println(list.map(x=>x+2)) // same as  _+2
    println(list.map(_+2))   //placeholder syntax each argument must be used
                              //exactly once

    //List(3, 4, 5) as output

    println("###flatMap transversed #### :")
    println(list.flatMap(x=>list.groupBy(x=>"sublist"))) //one to Many map by flatMap
    println(list.flatMap(x=>list.groupBy(x=>"list ")))
    //List(3, 4, 5) as output

    println("###Using filter #### :")
    println(list.filter(x=>x%2==1)) //return odd list List(1, 3)
    println(list.filter(_%2==1))

    println("###Using reduce #### :")
    println(list.reduce((x,y)=>x+y)) //return odd list List(1, 3)
    println(list.reduce(_+_))

    println(list.sum) //return odd list List(1, 3)
    println(list.sum)

  }
}
