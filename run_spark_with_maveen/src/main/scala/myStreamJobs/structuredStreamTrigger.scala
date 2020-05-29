package myStreamJobs
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.streaming.Trigger

object structuredStreamTrigger {
  
  def main (args: Array[String]) {
        
        val tempDir = System.getProperty("user.dir")
        val path = tempDir + "/warehouse"
        val spark = SparkSession.builder()
                                .appName("StructuredStreamWithSparkSQL")
                                .master("local[*]")
                                .config("spark.sql.warehouse.dir", path)
                                .getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")
        
        val schemaDef = new StructType()
                            .add("line_id",IntegerType)
                            .add("invoice_no",StringType)
                            .add("stock_code",StringType)
                            .add("description",StringType)
                            .add("quantity",IntegerType)
                            .add("invoice_date",DateType)
                            .add("unit_price",DoubleType)
                            .add("customer_id",StringType)
                            .add("country",StringType)
                            .add("invoice_ts",TimestampType)
                            
        val streamData = spark.readStream
                              .schema(schemaDef)
                              .option("header","true")
                              .option("maxFilesPerTrigger","2")
                              .csv("C:/SimulateStreaming/StructureStream")
                              
        //below we will use spark-SQL
        streamData.createOrReplaceTempView("insurance_data")
        
        //this is windowing using spark sql
        //slide duration- 15 minutes
        val tumblingWindowAgg = spark.sql("select "+
                                          "WINDOW(invoice_ts, '1 hour') as invoice_window, "+ 
                                          "country, "+
                                          "sum(unit_price) as total_price "+
                                          "from insurance_data "+
                                          "group by window(invoice_ts, '1 hour'), country "+
                                          "order by invoice_window desc"
                                         )
        
        val sink = tumblingWindowAgg.writeStream
                                    .trigger(Trigger.ProcessingTime("20 seconds"))
                                    //.trigger(Trigger.Once())
                                    .format("console")
                                    .option("truncate", "false")
                                    .outputMode(OutputMode.Complete())
                                    .start()
        sink.awaitTermination()
        
  }
}