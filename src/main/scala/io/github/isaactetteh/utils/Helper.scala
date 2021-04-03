package io.github.isaactetteh.utils

import com.amazonaws.services.kinesis.producer.{
  KinesisProducer,
  KinesisProducerConfiguration
}
import software.amazon.awssdk.services.kinesis.KinesisClient
import software.amazon.awssdk.services.kinesis.model._
import software.amazon.kinesis.retrieval.KinesisClientRecord
import twitter4j.conf.ConfigurationBuilder
import twitter4j.{TwitterException, TwitterObjectFactory}

import scala.collection.mutable
import scala.jdk.CollectionConverters._

object Helper {
  val STREAM_NAME = "tweets-stream"

  def createStream(client: KinesisClient, streamName: String): Unit = {
    try {
      val createStreamRequest = CreateStreamRequest.builder
        .streamName(streamName)
        .shardCount(2)
        .build()
      client.createStream(createStreamRequest)
    } catch {
      case e: KinesisException => println(e.getMessage)
    }
  }

  def deleteStream(client: KinesisClient, streamName: String): Unit = {
    val deleteStreamRequest =
      DeleteStreamRequest.builder().streamName(streamName).build()
    val response = client.deleteStream(deleteStreamRequest)
    println(response.responseMetadata())
  }

  def getTwitterConfigurationBuilder: ConfigurationBuilder = {
    val cb = new ConfigurationBuilder
    cb.setDebugEnabled(true)
      .setOAuthConsumerKey("")
      .setOAuthConsumerSecret("")
      .setOAuthAccessToken("")
      .setOAuthAccessTokenSecret("")
  }

  def getShards(
      kinesisClient: KinesisClient,
      streamName: String
  ): mutable.Buffer[Shard] = {
    val listShardsRequest: ListShardsRequest =
      ListShardsRequest.builder().streamName(streamName).build()

    var shards: ListShardsResponse = kinesisClient.listShards(listShardsRequest)
    val shardList: mutable.Buffer[Shard] = shards.shards().asScala

    while (shards.nextToken() != null) {
      shardList.appendAll(shards.shards().asScala)
      val listShardsRequest: ListShardsRequest = ListShardsRequest
        .builder()
        .streamName(streamName)
        .nextToken(shards.nextToken)
        .build()
      shards = kinesisClient.listShards(listShardsRequest)
    }
    shardList
  }

  def sleep(): Unit = {
    try {
      Thread.sleep(500)
    } catch {
      case e: InterruptedException => throw new RuntimeException(e)
    }
  }

  def processRecord(record: Any): Unit = {
    val tweetJson: String = record match {
      case r: Record               => r.data().asUtf8String()
      case kr: KinesisClientRecord => kr.data.array().map(_.toChar).mkString
      case _                       => "Unknown"
    }
    try {
      val tweet = TwitterObjectFactory.createStatus(tweetJson)
      println(tweet.getLang + " => " + tweet.getText)
    } catch {
      case _: TwitterException => throw new RuntimeException;
    }

  }

  def createKinesisProducer: KinesisProducer = {
    val config = new KinesisProducerConfiguration()
      .setRequestTimeout(60000)
      .setRecordMaxBufferedTime(15000)
      .setRegion("us-east-1")
    new KinesisProducer(config)
  }

}
