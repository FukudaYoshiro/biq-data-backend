package example

import twitter4j.auth.AccessToken
import twitter4j.{GeoLocation, Status, StatusListener, _}
import scala.collection.mutable.ArrayBuffer

object App {
    def main(args: Array[String]): Unit = {
        val streamFactory = new TwitterStreamFactory()
        val twitterStream = streamFactory.getInstance()
        twitterStream.setOAuthConsumer("rwcbIdCTJZ91ROpA0yluLtCPB", "CZixZElCoi7QhJBhX9NJjIOFYWCtZHa9QCS3x3gDlafgZFl8XY")
        val accessToken = new AccessToken("1664570156-zKRpnhJe8XN3iRSKs3FfIgVJ85EKZOGcbe82ci2", "t9t0AWDhiW3nmO3CcdLIrAheNOQmIGGqOrNO9buIhjxyh")
        twitterStream.setOAuthAccessToken(accessToken)
        twitterStream.addListener(new MyStatusListener())

        twitterStream.sample()
    }
}

class MyStatusListener extends StatusListener {
    override def onStatus(status: Status): Unit = {
        var lat: Double = 0
        var lng: Double = 0
        var urls: ArrayBuffer[String] = ArrayBuffer()
        var medias: ArrayBuffer[String] = ArrayBuffer()

        //. 位置情報が含まれていれば取得する
        val location: GeoLocation = status.getGeoLocation()
        if (location != null) {
            val dlat = location.getLatitude()
            val dlng = location.getLongitude()
            lat = dlat
            lng = dlng
        }
        var id = status.getId()
        var text = status.getText()
        var userid = status.getUser().getId()
        var username = status.getUser().getScreenName()

        var created = status.getCreatedAt()

        //. ツイート本文にリンクURLが含まれていれば取り出す
        var uentitys = status.getURLEntities()
        if (uentitys != null && uentitys.length > 0) {
            for (i <- 0 to uentitys.length) {
                var uentity = uentitys(i)
                var expandedURL = uentity.getExpandedURL()
                urls += expandedURL
            }
        }

        //. ツイート本文に画像／動画URLが含まれていれば取り出す
        var mentitys = status.getMediaEntities()
        if (mentitys != null && mentitys.length > 0) {
            var list: Array[String] = null
            for (i <- 0 to mentitys.length) {
                var mentity = mentitys(i)
                var expandedURL = mentity.getExpandedURL()
                medias += expandedURL
            }
        }

        //. 取り出した情報を表示する（以下では id, username, text のみ）
        println("id = " + id + ", username =" + username + "text =" + text)
    }

    override def onDeletionNotice(statusDeletionNotice: StatusDeletionNotice): Unit = {}

    override def onStallWarning(warning: StallWarning): Unit = {}

    override def onScrubGeo(userId: Long, upToStatusId: Long): Unit = {}

    override def onTrackLimitationNotice(numberOfLimitedStatuses: Int): Unit = {}

    override def onException(ex: Exception): Unit = {}
}
