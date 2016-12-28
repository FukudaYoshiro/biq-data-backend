package main

object Stopword {
    val stopwords = List(
        "フォロー",
        "ちゃん",
        "お願い",
        "ください",
        "可愛い",
        "くらい",
        "ござい",
        "ありがとう",
        "ながら",
        "www",
        "よろしく",
        "嬉しい",
        "なかっ",
        "..."
    )

    def notContains(word: String): Boolean = {
        return !stopwords.contains(word)
    }
}
