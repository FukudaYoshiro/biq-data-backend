name := "basic-project"

organization := "example"

version := "0.1.0-SNAPSHOT"

scalaVersion := "2.11.2"

crossScalaVersions := Seq("2.10.4", "2.11.2")

libraryDependencies ++= Seq(
    "org.twitter4j" % "twitter4j-core" % "4.0.4",
    "org.twitter4j" % "twitter4j-async" % "4.0.4",
    "org.twitter4j" % "twitter4j-stream" % "4.0.4",
    "org.twitter4j" % "twitter4j-media-supsport" % "4.0.4",
    "org.scalatest" %% "scalatest" % "2.2.1" % "test",
    "org.scalacheck" %% "scalacheck" % "1.11.5" % "test"
)

initialCommands := "import example._"
