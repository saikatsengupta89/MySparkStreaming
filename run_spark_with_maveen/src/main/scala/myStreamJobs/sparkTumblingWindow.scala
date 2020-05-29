package myStreamJobs
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.OutputMode

object sparkTumblingWindow {
    
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
        
        //slide duration- 15 minutes
        val tumblingWindowAgg = streamData.groupBy(
                                              window(timeColumn=col(colName="invoice_ts").alias("Invoice Time"), windowDuration="1 hours", 
                                                     slideDuration="15 minutes"),
                                              col(colName="country").alias("Country Name")
                                           ).agg (sum(col(colName="unit_price")).alias("Total Sales"))
                                            .orderBy("window")
                                          
        val sink = tumblingWindowAgg.writeStream
                                    .format(source="console")
                                    .option("truncate", "false")
                                    .outputMode(OutputMode.Complete())
                                    .start()
        sink.awaitTermination()
                               
    }
}