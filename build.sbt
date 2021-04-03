name := "KinesisExamples"

version := "0.1"

scalaVersion := "2.13.5"

idePackagePrefix := Some("io.github.isaactetteh")

libraryDependencies ++= Seq(
  "org.twitter4j" % "twitter4j-stream" % "4.0.7",
  "software.amazon.awssdk" % "kinesis" % "2.16.14",
  "software.amazon.kinesis" % "amazon-kinesis-client" % "2.3.2",
  "com.amazonaws" % "amazon-kinesis-producer" % "0.14.5",
  "org.slf4j" % "slf4j-simple" % "1.7.5"
)