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
        "...",
        "！　#",
        "ところ",
        "ある程度",
        "バンド",
        "そして",
        "\uD83D\uDE2D\uD83D",
        "おちん",
        "どちら",
        "ほんと",
        "みんな",
        "よかっ",
        "本当に",
        "なんか",
        "みたい",
        "めっちゃ",
        "できる",
        "楽しみ",
        "という",
        "ましょ",
        "初めて"
    )

    def notContains(word: String): Boolean = {
        return !stopwords.contains(word)
    }
}
