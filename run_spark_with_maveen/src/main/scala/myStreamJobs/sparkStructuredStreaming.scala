package myStreamJobs
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.streaming.OutputMode

object sparkStructuredStreaming {
  
      def main (args: Array[String] ) {
        
          val tempDir = System.getProperty("user.dir")
          val path = tempDir + "/warehouse"
          val spark= SparkSession.builder()
                                 .appName("Working with Structured Streaming")
                                 .master("local[*]")
                                 .config("spark.sql.warehouse.dir", path)
                                 .getOrCreate()
          
          spark.sparkContext.setCheckpointDir("/SimulateStreaming/checkpoints")
          spark.sparkContext.setLogLevel(logLevel="ERROR")
          
          val retailDataSchema = new StructType()
                                     .add (name="LineID", IntegerType)
                                     .add (name="InvoiceNo", StringType)
                                     .add (name="StockCode", StringType)
                                     .add (name="Description", StringType)
                                     .add (name="Quantity", StringType)
                                     .add (name="InvoiceDate", DateType)
                                     .add (name="UnitPrice", StringType)
                                     .add (name="CustomerID", StringType)
                                     .add (name="Country", StringType)
                                     .add (name="InvoiceTimestamp", TimestampType)
          
          val streamingData = spark.readStream
                                   .option("header","true")
                                   .schema (retailDataSchema)
                                   .csv("C:/SimulateStreaming/StructureStream")
          
          //below we are using dataset APIs
          val filteredData = streamingData.filter(conditionExpr="Country in ('United Kingdom', 'France')")
                                          .where (conditionExpr="Quantity >= 10")
                                          .select("InvoiceNo","StockCode","Quantity","UnitPrice","Country")
          
          val query = filteredData.writeStream
                                  .format(source="console")
                                  .queryName(queryName="selected sales")
                                  .outputMode(OutputMode.Update())
                                  .start() //start the streaming process with this clause
          
          query.awaitTermination()
        
      }
}