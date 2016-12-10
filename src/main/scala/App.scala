package main

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.SparkContext._
import org.apache.spark.streaming.twitter._
import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext._
import org.apache.log4j.{Level, Logger}

object App {
    def main(args: Array[String]): Unit = {
        Logger.getLogger("org").setLevel(Level.OFF)

        // Set Twitter Access Keys
        val config = new java.util.Properties
        config.load(this.getClass().getClassLoader().getResourceAsStream("config.properties"))
        System.setProperty("twitter4j.oauth.consumerKey", config.get("twitter_consumerKey").toString)
        System.setProperty("twitter4j.oauth.consumerSecret", config.get("twitter_consumerSecret").toString)
        System.setProperty("twitter4j.oauth.accessToken", config.get("twitter_accessToken").toString)
        System.setProperty("twitter4j.oauth.accessTokenSecret", config.get("twitter_accessTokenSecret").toString)

        // Create Stream
        val filters = Array("Coffee", "Tea", "Alcohol")
        val sparkConf = new SparkConf().setAppName("TwitterPopularTags").setMaster("local[*]")
        val ssc = new StreamingContext(sparkConf, Seconds(2))
        val stream = TwitterUtils.createStream(ssc, None, filters)

        // Get RDD that has hashtags
        val hashTags = stream.flatMap(status => status.getText.split(" ").filter(_.startsWith("#")))

        // Get DStream
        val topCounts60 = hashTags.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(60))
            .map{case (topic, count) => (count, topic)}
            .transform(_.sortByKey(false))

        val topCounts10 = hashTags.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(10))
            .map{case (topic, count) => (count, topic)}
            .transform(_.sortByKey(false))

        // Print popular hashtags
        topCounts60.foreachRDD(rdd => {
            val topList = rdd.take(10)
            println("\nPopular topics in last 60 seconds (%s total):".format(rdd.count()))
            topList.foreach{case (count, tag) => println("%s (%s tweets)".format(tag, count))}
        })

        topCounts10.foreachRDD(rdd => {
            val topList = rdd.take(10)
            println("\nPopular topics in last 10 seconds (%s total):".format(rdd.count()))
            topList.foreach{case (count, tag) => println("%s (%s tweets)".format(tag, count))}
        })

        println("\n====================== Start. ======================")
        ssc.start()
        ssc.awaitTermination()
    }
}
