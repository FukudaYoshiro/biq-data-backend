package main

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.SparkContext._
import org.apache.spark.streaming.twitter._
import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext._
import org.apache.log4j.{Level, Logger}
import org.atilika.kuromoji.{Token, Tokenizer}
import java.sql.{Connection, ResultSet, Statement}

import org.apache.commons.dbcp2._

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
        val lanFilter = stream.filter(status => status.getUser().getLang == "ja")
        val connection = Datasource.connectionPool.getConnection
        val stmt = connection.createStatement()

        // Twitterから取得したツイートを処理する
        val tweetStream = lanFilter.flatMap(status => {
            val tokenizer: Tokenizer = Tokenizer.builder().build()
            val features: scala.collection.mutable.ArrayBuffer[String] = new collection.mutable.ArrayBuffer[String]()
            var tweetText: String = status.getText() //ツイート本文の取得

            // 不要な文字列の削除
            tweetText = tweetText
                .replaceAll("http(s*)://(.*)/", "")
                .replaceAll("\\uff57", "") // 全角の「ｗ」は邪魔www
                .replaceAll("\\p{javaWhitespace}", "") // 空白文字

            // ツイート本文の解析
            val tokens: java.util.List[Token] = tokenizer.tokenize(tweetText) // 形態素解析
            for (index <- 0 to tokens.size() - 1) {
                //各形態素に対して。。。
                val token = tokens.get(index)
                // 文字数が3文字以上
                if (token.getSurfaceForm().length() >= 3 && Stopword.notContains(token.getSurfaceForm())) {
                    // 条件に一致した形態素解析の結果を登録
                    features += (token.getSurfaceForm())
                }
            }
            features
        })

        // ウインドウ集計（行末の括弧の位置はコメントを入れるためです、気にしないで下さい。）
        val topCounts60 = tweetStream.map((_, 1) // 出現回数をカウントするために各単語に「1」を付与
        ).reduceByKeyAndWindow(_ + _, Seconds(60 * 60) // ウインドウ幅(60*60sec)に含まれる単語を集める
        ).map { case (topic, count) => (count, topic) // 単語の出現回数を集計
        }.transform(_.sortByKey(false)) // ソート

        // 出力
        topCounts60.foreachRDD(foreachFunc = rdd => {
            // 出現回数上位100単語を取得
            val topList = rdd.take(100)
            // コマンドラインに出力
            println("\n Popular topics in last 60*60 seconds (%s words):".format(rdd.count()))
            topList.foreach {
                case (count, tag) => {
                    println(s"$tag ($count tweets)")
                }
            }
            // DBにINSERTする形式にデータをマッピング
            // [ranking_id,[[word, count], [word, count], [word, count]]

            // rankingテーブルへINSERT,ranking_idを発行
            stmt.executeUpdate("INSERT INTO rankings (created_at, updated_at) VALUES (now(), now())", Statement.RETURN_GENERATED_KEYS)
            val rankingRes = stmt.executeQuery("SELECT id FROM rankings ORDER BY id DESC LIMIT 1")

            var rankingId :Int = 0
            if (rankingRes.next()) {
                rankingId = rankingRes.getInt("id")
            }

            var insertMap: scala.collection.mutable.Map[Int, Int] = scala.collection.mutable.Map()
            // keywordをkeyword_idに変換
            topList.foreach {
                case (count, tag) =>
                    // if keywordがdbに無ければ
                    val keywordRes: ResultSet = stmt.executeQuery(s"SELECT id from keywords where keyword = '$tag'")
                    if (keywordRes.next()){
                        insertMap += keywordRes.getInt(1) -> count
                    } else {
                        // keywordsテーブルへINSERT
                        stmt.executeUpdate(s"INSERT INTO keywords (keyword, created_at, updated_at) VALUES ('$tag', now(), now())")
                        val keywordIdRes = stmt.executeQuery("SELECT id FROM keywords ORDER BY id DESC LIMIT 1")
                        if (keywordIdRes.next()) {
                            insertMap += keywordIdRes.getInt(1) -> count
                        }
                    }
            }
            // ranking_itemsテーブルへranking_id, keyword_id, countをINSERT
            insertMap.foreach {
                case (keywordId, count) =>
                    stmt.executeUpdate(s"INSERT INTO ranking_items (ranking_id, keyword_id, count, created_at, updated_at) VALUES ($rankingId, $keywordId, $count, now(), now())")
            }
        })

        // 定義した処理を実行するSpark Streamingを起動！
        ssc.start()
        ssc.awaitTermination()
    }
}

object Datasource {
    val dbUrl = "jdbc:postgresql://localhost:5432/otsukimi"
    val connectionPool = new BasicDataSource()

    connectionPool.setUsername("ryota.murakami")
    connectionPool.setPassword("")

    connectionPool.setDriverClassName("org.postgresql.Driver")
    connectionPool.setUrl(dbUrl)
    connectionPool.setInitialSize(3)
}
