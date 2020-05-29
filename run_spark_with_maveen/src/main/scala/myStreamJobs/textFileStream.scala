package myStreamJobs

import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds

object textFileStream {
  
    def main (args: Array[String]) {
        
        val conf = new SparkConf().setMaster("local[*]")
                                  .setAppName("Streaming DStream Text File")
        
        //val sc  = new SparkContext(conf)
        val ssc = new StreamingContext(conf, batchDuration= Seconds(30))
        
        val streamRDD= ssc.textFileStream("C:/SimulateStreaming/InputStreamDir/")
        //val streamRDD= sc.textFile("/Scala Workspace/StreamDir/", 2)
        
        streamRDD.count().print()
        val filteredRDD= streamRDD.filter(line => {
                                      val firstRow= line.head
                                      line != firstRow
                                  })
                                  .map(line => {
                                      val items= line.split(",")
                                      (items(0).toString(), items(1).toString(), items(2).toString(), items(3).toString(), 
                                       items(5).toString(), items(6).toString(), items(7).toString())
                                   })
                                   //.filter(_._7.toUpperCase()=="FRANCE")
                                   
        filteredRDD.saveAsTextFiles("C:/SimulateStreaming/OutputStreamDir/", "txt")
        
        ssc.start()
        ssc.awaitTermination()
    }
}