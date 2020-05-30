package myStreamJobs
import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import twitter4j.conf.ConfigurationBuilder
import twitter4j.auth.OAuthAuthorization
import org.apache.spark.streaming.twitter.TwitterUtils

object twitterDStream {
  
  def main (args: Array[String]) {
      
      val conf = new SparkConf().setAppName("Spark DStream Example")
                                .setMaster("local[*]")
                                
      val ssc = new StreamingContext(conf, batchDuration=Seconds(5))
      
      //this will create a DStream which is nothing but a series of RDDs
      val consumerKey= "eazIPcImapoIi8B7VOKjp6O7b"
      val consumerSecret= "JPsaCzO90YoFUMcIgQF2W5s032cFaJnUFJM4fgqBA7x6vhYxmy"
      val accessToken= "760176198966280192-Tn7QfNkBHzWPVWzaB10TLab3IK8cAUw"
      val accessTokenSecret= "sW90lbSSlV5iDbFp16aZT9XCkwWjtSCeiE0RnMlN1tgwo"
      
      val filters = List("python")
      val cb = new ConfigurationBuilder
      cb.setDebugEnabled(true).setOAuthConsumerKey(consumerKey)
        .setOAuthConsumerSecret(consumerSecret)
        .setOAuthAccessToken(accessToken)
        .setOAuthAccessTokenSecret(accessTokenSecret)
      
      val auth = new OAuthAuthorization(cb.build)
      val tweets = TwitterUtils.createStream(ssc, Some(auth), filters).map(x=> x.getId.toString()+","+ x.getText().toString()) 
      tweets.print()
      
      ssc.start()
      ssc.awaitTermination()
                                
  }
}

/**************************** TWITTER KEYS **************************************/
//API key: eazIPcImapoIi8B7VOKjp6O7b
//API secret key: JPsaCzO90YoFUMcIgQF2W5s032cFaJnUFJM4fgqBA7x6vhYxmy
//Access token : 760176198966280192-Tn7QfNkBHzWPVWzaB10TLab3IK8cAUw
//Access token secret : sW90lbSSlV5iDbFp16aZT9XCkwWjtSCeiE0RnMlN1tgwo
/*********************************************************************************/