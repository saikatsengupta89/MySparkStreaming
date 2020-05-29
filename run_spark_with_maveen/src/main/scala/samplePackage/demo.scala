package samplePackage

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object demo {
  def main(args: Array[String]) {
    val sparkConf= new SparkConf().setAppName("MyFirstMavenApp")
                              .setMaster("local[*]")
           
    val sparkContext= new SparkContext(sparkConf)
    val r1= sparkContext.range(1, 100)
    r1.collect().foreach(println)
    
    sparkContext.stop()
  }
}