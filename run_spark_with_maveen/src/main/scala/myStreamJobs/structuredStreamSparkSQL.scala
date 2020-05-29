package myStreamJobs
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.OutputMode

object structuredStreamSparkSQL {
    
    def main (args : Array[String]) {
      
        val tempDir = System.getProperty("user.dir")
        val path = tempDir + "/warehouse"
        val spark = SparkSession.builder()
                                .appName("StructuredStreamWithSparkSQL")
                                .master("local[*]")
                                .config("spark.sql.warehouse.dir", path)
                                .getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")
        spark.sparkContext.setCheckpointDir("/SimulateStreaming/checkpoints")
                                
        val schemaDef = new StructType()
                            .add (name="line_id", IntegerType)
                            .add (name="invoice_no", StringType)
                            .add (name="stock_code", StringType)
                            .add (name="description", StringType)
                            .add (name="quantity", IntegerType)
                            .add (name="invoice_date", DateType)
                            .add (name="unit_price", DoubleType)
                            .add (name="customer_id", StringType)
                            .add (name="country", StringType)
                            .add (name="invoice_ts", TimestampType)
                            
        val streamingData = spark.readStream
                                 .format("com.databricks.spark.csv")
                                 .schema(schemaDef)
                                 .option("header","true")
                                 .option("maxFilesPerTrigger", 2)
                                 .csv("C:/SimulateStreaming/StructureStream")
                                 
        //below we will use spark-SQL
        streamingData.createOrReplaceTempView("insurance_data")
        
        val insurance_data_t1 = spark.sql ("select "+
                                           "invoice_no, "+
                                           "invoice_date, "+
                                           "description, "+
                                           "quantity, "+
                                           "unit_price, "+
                                           "customer_id, "+
                                           "country "+
                                           "from insurance_data "+
                                           "where upper(country) in ('UNITED KINGDOM','FRANCE') "+
                                           "and quantity >=10 "
                                           )
        
        insurance_data_t1.createOrReplaceTempView("insurance_data")                                   
        
        val insurance_data_t2 = spark.sql ("select "+
                                           "invoice_date, "+
                                           "country, "+
                                           "sum(unit_price) as total_price "+
                                           "from insurance_data "+
                                           "group by invoice_date, country"
                                      )
        
        val stream= insurance_data_t2.writeStream
                                     .format(source="console")
                                     .outputMode(OutputMode.Complete())
                                     .start()
                         
        stream.awaitTermination()
    }
    
}