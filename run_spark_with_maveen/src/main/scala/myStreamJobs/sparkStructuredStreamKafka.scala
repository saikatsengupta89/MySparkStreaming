package myStreamJobs
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger

import org.apache.spark.sql.streaming.OutputMode

object sparkStructuredStreamKafka {
     
    def main (args: Array[String]) {
        
        val tempDir = System.getProperty("user.dir")
        val path = tempDir + "/warehouse"
        val spark = SparkSession.builder()
                                .appName("StructuredStreamWithSparkSQL")
                                .master("local[*]")
                                .config("spark.sql.warehouse.dir", path)
                                .getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")
        
        val KAFKA_TOPIC_NAME="test_spark_read"
        val KAFKA_BOOTSTRAP_SERVER="127.0.0.1:9092"
        
        val schemaDef = new StructType()
                         .add("transaction_id",StringType)
                         .add("transaction_card_type",StringType)
                         .add("transaction_amount",DoubleType)
                         .add("transaction_datetime",TimestampType)
                         
        
        val kafkaStreamDF = spark.readStream
                                   .format("kafka")
                                   .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVER)
                                   .option("subscribe", KAFKA_TOPIC_NAME)
                                   //.option("startingOffsets", "latest")
                                   .option("startingOffsets", "earliest")
                                   .load()
                                   
        // Select data(value column to string format)
        val kafkaStreamSTG1 = kafkaStreamDF.select(col("value")
                                                  .cast("string"))
        
        
        val kafkaStreamSTG2 = kafkaStreamSTG1.select(from_json(col("value"), schemaDef).alias("data"))
                                             .select("data.*")
                                             
        
        kafkaStreamSTG2.registerTempTable("transaction_data") 
        
        val kafkaStreamSTG3 = spark.sql (
                                "select "+
                                "transaction_datetime, "+
                                "from_unixtime(unix_timestamp(transaction_datetime, 'yyyy-MM-dd HH:MM:SS'),'yyyyMMdd') as time_key, "+
                                "transaction_id, "+
                                "transaction_card_type, "+
                                "transaction_amount, "+
                                "case when transaction_amount > 500 then 'TXN GT THAN 500' else 'TXN WITHIN 500' end as txn_class "+
                                "from transaction_data"
                              )
                                   
        val sink = kafkaStreamSTG3.writeStream
                                  .format(source="console")
                                  .option("truncate", "false")
                                  .outputMode(OutputMode.Append())
                                  .trigger(Trigger.ProcessingTime("20 seconds"))
                                  .start()
        
        sink.awaitTermination()
        
        
    }
}