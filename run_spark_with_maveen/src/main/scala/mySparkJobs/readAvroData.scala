package mySparkJobs
import org.apache.spark.sql.SparkSession
import com.databricks.spark.avro._
import org.apache.spark.sql.SaveMode

object readAvroData {
  
  def main (args: Array[String]) {
      val spark = SparkSession.builder()
                              .master("local[*]")
                              .appName("readAvroData")
                              .getOrCreate()
                              
      val avroDF = spark.read
                        .format("com.databricks.spark.avro")
                        .load(path="C:/Scala Workspace/data/twitter.snappy.avro")
                        .toDF("user_name","tweet","timestamp")
      
      avroDF.printSchema()
      avroDF.createOrReplaceTempView("tweets")
      
      val avroTransformedData= spark.sql(
          "select "+
          "user_name, "+
          "tweet, "+
          "from_unixtime(timestamp, 'dd-MMM-yyyy HH:MM:SS') date_time "+
          "from tweets"
      )
      
      avroTransformedData.show(false)
      avroTransformedData.write
                         .format("com.databricks.spark.avro")
                         .mode(SaveMode.Overwrite)
                         .save(path="C:/Scala Workspace/data/data_avro")
  }
}