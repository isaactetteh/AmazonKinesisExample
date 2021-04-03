package io.github.isaactetteh.sdk_examples

import io.github.isaactetteh.utils.Helper
import software.amazon.awssdk.services.kinesis.KinesisClient
import software.amazon.awssdk.services.kinesis.model._

import scala.jdk.CollectionConverters._

object TweetConsumerSdk {
  def main(args: Array[String]): Unit = {
    val streamName = Helper.STREAM_NAME
    val kinesisClient: KinesisClient = KinesisClient.create
    val shard = Helper.getShards(kinesisClient, streamName)


    val getShardIteratorRequest: GetShardIteratorRequest = GetShardIteratorRequest
      .builder.streamName(streamName)
      .shardId(shard.head.shardId)
      .shardIteratorType("TRIM_HORIZON")
      .build

    val getShardIteratorResponse = kinesisClient.getShardIterator(getShardIteratorRequest)
    var shardIterator: String = getShardIteratorResponse.shardIterator

    var hasNextShardIterator = true
    while (hasNextShardIterator) {
      val getRecordsRequest = GetRecordsRequest.builder.shardIterator(shardIterator).build
      val result = kinesisClient.getRecords(getRecordsRequest)
      val records = result.records.asScala
      records.foreach(r => Helper.processRecord(r))
      Helper.sleep()
      shardIterator = result.nextShardIterator
      hasNextShardIterator = result.hasRecords
    }
  }

}
