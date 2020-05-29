package mySparkJobs
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object parseGender {
  
    def classifyGenderFlag(s:String): Int= {
      val str= s.toLowerCase()
            if (List("cis female", "f", "female", "woman", "femake", "female ", "cis-female/femme", 
                      "female (cis)", "femail"). contains(str)) {
                0
            }
            else if (List("male", "m", "male-ish", "maile", "mal", "male (cis)", "make", "male ", "man", "msle", 
                          "mail", "malr", "cis man", "cis male").contains(str)) {
                1
            }
            else {
                -1
            }
    }
  
    def main (args : Array [String]) {
      
        val spark = SparkSession.builder()
                                .appName("createParseGenderUDF")
                                .master("local[*]")
                                .getOrCreate()
                                
        val survey_df = spark.read
                             .format("csv")
                             .option("header","true")
                             .option("inferSchema","true")
                             .load("C:/Data Engineer/SampleData/Survey.csv")
        survey_df.registerTempTable("survey_data")
        
        val parseGender =  (s: String)  => {
            val str= s.toLowerCase()
            if (List("cis female", "f", "female", "woman", "femake", "female ", "cis-female/femme", 
                      "female (cis)", "femail"). contains(str)) {
                "Female"
            }
            else if (List("male", "m", "male-ish", "maile", "mal", "male (cis)", "make", "male ", "man", "msle", 
                          "mail", "malr", "cis man", "cis male").contains(str)) {
                "Male"
            }
            else {
                "Transgender"
            }
        }
        
        //another way of specifying a function and registering it to a spark sesion
        val parseGenderFlag = classifyGenderFlag(_) //partially applied function
        
        //Registering the function with the spark session
        spark.udf.register("getGender", parseGender)
        spark.udf.register("getGenderFlag", parseGenderFlag)
        
        //Apply the UDF created
        val enrichData = spark.sql (
            "SELECT "+
            "PID, "+
            "NAME, "+
            "GENDER, "+
            "getGender(GENDER) GENDER_CLASS, "+
            "getGenderFlag(GENDER) GENDER_FLAG "+
            "from survey_data"
            )
        enrichData.show()
    }
  
}