package mySparkJobs
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
object sparkUDFWithCurry {
  
  // define a normal scala function
  // which returns a normal UDF function
  def diffCalculation(roundingPrecession:Int): UserDefinedFunction= {
      return udf((close:Double, prevClose:Double) => {
                BigDecimal(close - prevClose).setScale(roundingPrecession, BigDecimal.RoundingMode.HALF_UP).toDouble
             })
  }
  
  def main (args: Array[String]) {
      
      val spark= SparkSession.builder()
                             .appName(name= "Build UDF With Curry Functions")
                             .master("local[*]")
                             .getOrCreate()
      
      val roundingPrecession= if (args(0).toInt == null) 0 else args(0).toInt
      val data= spark.read
                     .format("com.databricks.spark.csv")
                     .option("header","true")
                     .option("delimiter",",")
                     .option("inferSchema","true")
                     .load("C:/Scala Workspace/data/nse-stocks-data.csv")
      
      data.printSchema()
      
      val dataT1= data.withColumn(colName="DIFFCLOSE", diffCalculation(roundingPrecession)(data("CLOSE"), data("PREVCLOSE")))
      dataT1.show(10, false)
      
      
  }
}