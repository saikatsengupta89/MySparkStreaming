package myStreamJobs
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.functions._

object sparkStructuredStreamingAgg {
  
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
                                     .add (name="UnitPrice", DoubleType)
                                     .add (name="CustomerID", StringType)
                                     .add (name="Country", StringType)
                                     .add (name="InvoiceTimestamp", TimestampType)
          
          /*
            makFilesPerTrigger will make spark process only 2 files per batch
            if there are 5 files in a folder, it will produce output in 3 batches: (2,2,1)
            since no window is specified, spark will consider processing all the files available
            under mentioned path in one single window into multiple batches based on maxFilesPerTrigger
          */
          val streamingData = spark.readStream
                                   .option("header","true")
                                   .option("maxFilesPerTrigger",2)
                                   .schema (retailDataSchema)
                                   .csv("C:/SimulateStreaming/StructureStream")
          
          //below we are using dataset APIs
          val filteredData = streamingData.filter(conditionExpr="Country in ('United Kingdom', 'France')")
                                          .where (conditionExpr="Quantity >= 10")
                                          .select("InvoiceNo","InvoiceDate", "StockCode","Quantity","UnitPrice","Country")
          
          val aggregateData =  filteredData.groupBy(col1="InvoiceDate", cols="Country")
                                           .agg(sum(columnName="UnitPrice").alias("TotalPrice"))
          
          val query = aggregateData.writeStream
                                   .format(source="console")
                                   .queryName(queryName="aggregate sales figure")
                                   .outputMode(OutputMode.Complete())
                                   .start() //start the streaming process with this clause
          
          query.awaitTermination()
        
      }
}