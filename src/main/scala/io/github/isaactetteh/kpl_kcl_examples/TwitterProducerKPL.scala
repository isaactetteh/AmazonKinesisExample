package io.github.isaactetteh.kpl_kcl_examples

import com.amazonaws.services.kinesis.producer.{
  KinesisProducer,
  UserRecordFailedException,
  UserRecordResult
}
import com.google.common.util.concurrent.{
  FutureCallback,
  Futures,
  ListenableFuture
}
import io.github.isaactetteh.utils.Helper
import software.amazon.awssdk.services.kinesis.KinesisClient
import twitter4j.{RawStreamListener, TwitterObjectFactory, TwitterStreamFactory}

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.util.concurrent.{ExecutorService, Executors}
import scala.jdk.CollectionConverters._

object TwitterProducerKPL {
  val executor: ExecutorService = Executors.newFixedThreadPool(1)
  def main(args: Array[String]): Unit = {
    val streamName = Helper.STREAM_NAME
    // val kinesisClient = KinesisClient.create()
    // Helper.createStream(kinesisClient, streamName)

    val cb = Helper.getTwitterConfigurationBuilder
    val twitterStream = new TwitterStreamFactory(cb.build()).getInstance()
    val producer =
      new TweetStatusListener(Helper.createKinesisProducer, streamName)
    twitterStream.addListener(producer)
    twitterStream.sample()
  }

  class TweetStatusListener(
      kinesisProducer: KinesisProducer,
      streamName: String
  ) extends RawStreamListener {

    override def onMessage(rawString: String): Unit = {
      try {
        val status = TwitterObjectFactory.createStatus(rawString)
        if (status.getUser != null) {
          val tweetBytes = rawString.getBytes(StandardCharsets.UTF_8)
          val future: ListenableFuture[UserRecordResult] = kinesisProducer
            .addUserRecord(
              streamName,
              status.getLang,
              ByteBuffer.wrap(tweetBytes)
            )
          Futures.addCallback(
            future,
            new FutureCallback[UserRecordResult] {
              override def onSuccess(result: UserRecordResult): Unit = {

                println(
                  s"${status.getLang} -> ${result.getShardId}, ${result.getSequenceNumber}"
                )
              }

              override def onFailure(t: Throwable): Unit = {
                t match {
                  case recordFailed: UserRecordFailedException =>
                    println(
                      s"Put failed - ${recordFailed.getResult.getAttempts.asScala.last.getErrorMessage}"
                    )
                  case _ => println(s"Unknown - ${t.getMessage}")
                }
              }
            },
            executor
          )
        }
      } catch {
        case e: Exception =>
          e.printStackTrace()
      }
    }

    override def onException(ex: Exception): Unit = ex.printStackTrace()
  }

}
