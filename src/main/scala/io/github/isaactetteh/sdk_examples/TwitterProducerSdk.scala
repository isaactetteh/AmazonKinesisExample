package io.github.isaactetteh.sdk_examples

import io.github.isaactetteh.utils.Helper
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.services.kinesis.KinesisClient
import software.amazon.awssdk.services.kinesis.model.PutRecordRequest
import twitter4j.{RawStreamListener, TwitterObjectFactory, TwitterStreamFactory}

import java.nio.charset.StandardCharsets


object TwitterProducerSdk {
  def main(args: Array[String]): Unit = {
    val streamName = Helper.STREAM_NAME
    val kinesisClient = KinesisClient.create()
    Helper.createStream(kinesisClient, streamName)
    val cb = Helper.getTwitterConfigurationBuilder
    val twitterStream = new TwitterStreamFactory(cb.build()).getInstance()
    twitterStream.addListener(new TweetStatusListener(kinesisClient, streamName))
    twitterStream.sample()
//    Helper.deleteStream(kinesisClient, streamName)
  }

  class TweetStatusListener(val kinesisClient: KinesisClient, streamName: String) extends RawStreamListener {
    override def onMessage(rawString: String): Unit = {
      try {
        val status = TwitterObjectFactory.createStatus(rawString)
        if (status.getUser != null) {
          val tweetBytes = rawString.getBytes(StandardCharsets.UTF_8)
          val putRecordRequest = PutRecordRequest.builder
            .streamName(streamName)
            .partitionKey(status.getLang)
            .data(SdkBytes.fromByteArray(tweetBytes))
            .build
          val putRecordResponse = kinesisClient.putRecord(putRecordRequest)
          println(status.getLang + " -> " + putRecordResponse)
        }
      } catch {
        case e: Exception =>
          e.printStackTrace()
      }
    }

    override def onException(ex: Exception): Unit = {
      ex.printStackTrace()
    }
  }

}
