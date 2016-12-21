package main

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.SparkContext._
import org.apache.spark.streaming.twitter._
import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext._
import org.apache.log4j.{Level, Logger}
import org.atilika.kuromoji.{Token, Tokenizer}
import java.util.regex._

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
        val sparkConf = new SparkConf().setAppName("TwitterPopularTags").setMaster("local[*]")
        val ssc = new StreamingContext(sparkConf, Seconds(2))
        val stream = TwitterUtils.createStream(ssc, None)
        // ググって出てくる情報ではユーザーの言語で絞り込む設定はしていないので
        val lanFilter = stream.filter(status => status.getUser().getLang == "ja")

        // Twitterから取得したツイートを処理する
        val tweetStream = lanFilter.flatMap(status => {
            val tokenizer : Tokenizer = Tokenizer.builder().build()  // kuromojiの分析器
            val features : scala.collection.mutable.ArrayBuffer[String] = new collection.mutable.ArrayBuffer[String]() //解析結果を保持するための入れ物
            var tweetText : String = status.getText() //ツイート本文の取得

            val filter_pattern : Pattern = Pattern.compile("^[ぁ-んァ-ヶーa-zA-Z0-9一-龠０-９、。\n\r]+$") //「日本語、英数字、寛容的記号」の正規表現
            if(filter_pattern.matcher(tweetText).find()) {  // ひらがなが含まれているツイートのみ処理
                // 不要な文字列の削除
                tweetText = tweetText.replaceAll("http(s*)://(.*)/", "").replaceAll("¥¥uff57", "") // 全角の「ｗ」は邪魔www

                // ツイート本文の解析
                val tokens : java.util.List[Token] = tokenizer.tokenize(tweetText) // 形態素解析
                val pattern : Pattern = Pattern.compile("^[a-zA-Z]+$|^[0-9]+$") //「英数字か？」の正規表現
                for(index <- 0 to tokens.size()-1) { //各形態素に対して。。。
                val token = tokens.get(index)
                    val matcher : Matcher = pattern.matcher(token.getSurfaceForm())
                    // 文字数が3文字以上で、かつ、英数字のみではない単語を検索
                    if(token.getSurfaceForm().length() >= 3 && !matcher.find()) {
                        // 条件に一致した形態素解析の結果を登録
                        features += (token.getSurfaceForm())
                    }
                }
            }
            features
        })

        // ウインドウ集計（行末の括弧の位置はコメントを入れるためです、気にしないで下さい。）
        val topCounts60 = tweetStream.map((_, 1)                      // 出現回数をカウントするために各単語に「1」を付与
        ).reduceByKeyAndWindow(_+_, Seconds(60*60)   // ウインドウ幅(60*60sec)に含まれる単語を集める
        ).map{case (topic, count) => (count, topic)  // 単語の出現回数を集計
        }.transform(_.sortByKey(false))               // ソート

        // 出力
        topCounts60.foreachRDD(rdd => {
            // 出現回数上位20単語を取得
            val topList = rdd.take(20)
            // コマンドラインに出力
            println("¥n Popular topics in last 60*60 seconds (%s words):".format(rdd.count()))
            topList.foreach{case (count, tag) => println(s"$tag ($count tweets)")}
        })

        // 定義した処理を実行するSpark Streamingを起動！
        ssc.start()
        ssc.awaitTermination()
    }
}
