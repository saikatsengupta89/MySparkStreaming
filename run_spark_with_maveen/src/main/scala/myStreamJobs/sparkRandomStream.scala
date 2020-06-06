package myStreamJobs
import java.util.Random
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, udf, rand}
import org.apache.spark.sql.streaming.OutputMode
object sparkRandomStream {
  
//  def randomStates():String= {
//    random.next
//  }
//  
  def main (args:Array[String]) {
    
    val spark= SparkSession.builder()
                           .master("local[*]")
                           .appName("checkDynamicUDF")
                           .getOrCreate()
    
    val random = new Random()
    
    val states = Seq("CA", "TX", "NY", "IA")
    val randomState = udf(() => states(random.nextInt(states.length)))
  
    val streamData = spark.readStream.format("rate").option("rowsPerSecond", 5).load()
                          .withColumn("loan_id", col("value") + 10000)
                          .withColumn("funded_amnt", (rand() * 5000 + 5000).cast("integer"))
                          .withColumn("paid_amnt", col("funded_amnt") - (rand() * 2000))
                          .withColumn("addr_state", randomState())
    val stream= streamData.writeStream
                          .format(source="console")
                          .option("truncate", "false")
                          .outputMode(OutputMode.Append())
                          .start()
    stream.awaitTermination()          
    
  }
}