package mySparkJobs
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import com.databricks.spark.csv
object readCSV {
  def main(args: Array[String]) {
    val spark= SparkSession.builder()
                           .appName(name= "read a csv file")
                           .master("local[*]")
                           .getOrCreate() 
                           
    val retailDataSchema = new StructType()
                                     .add (name="InvoiceNo", StringType)
                                     .add (name="StockCode", StringType)
                                     .add (name="Description", StringType)
                                     .add (name="Quantity", IntegerType)
                                     .add (name="InvoiceDate", TimestampType)
                                     .add (name="UnitPrice", DoubleType)
                                     .add (name="CustomerID", StringType)
                                     .add (name="Country", StringType)
                                     .add (name="InvoiceTimestamp", TimestampType)
                                     
    val data= spark.read
                   .option("header","true")
                   .schema(retailDataSchema)
                   .csv("C:/SimulateStreaming/StructureStream")
    data.show(false) 
    data.printSchema()
  }
  
}