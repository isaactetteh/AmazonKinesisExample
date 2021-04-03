package io.github.isaactetteh.sdk_examples

import io.github.isaactetteh.utils.Helper
import software.amazon.awssdk.services.kinesis.{KinesisAsyncClient, KinesisClient}
import software.amazon.awssdk.services.kinesis.model._

import scala.jdk.CollectionConverters._

object TwitterConsumerEFO {
  /* register consumer
    wait for the consumer to register
    start positioning
    subscribe to shard
    process received messages
   */
  def main(args: Array[String]): Unit = {
    val kinesisAsyncClient: KinesisAsyncClient = KinesisAsyncClient.create()
    val client = KinesisClient.create()

    val consumer: Consumer = registerConsumer(kinesisAsyncClient)

    waitForConsumerToRegister(kinesisAsyncClient, consumer)
    val shards = Helper.getShards(client,Helper.STREAM_NAME)
    val startingPosition: StartingPosition = StartingPosition
      .builder()
      .`type`(ShardIteratorType.TRIM_HORIZON)
      .build()
    val subscribeToShardRequest: SubscribeToShardRequest = SubscribeToShardRequest
      .builder()
      .consumerARN(consumer.consumerARN())
      .shardId(shards.head.shardId())
      .startingPosition(startingPosition)
      .build()

    while (true) {
      println("Subscribing to a shard")
      val recordsHandler = SubscribeToShardResponseHandler
        .builder
        .onError((t: Throwable) => println("Subscribe to shard error: " + t.getMessage))
        .subscriber(new RecordsProcessor)
        .build

      val future = kinesisAsyncClient.subscribeToShard(subscribeToShardRequest, recordsHandler)
      future.join

    }
  }

  @throws[InterruptedException]
  @throws[java.util.concurrent.ExecutionException]
  def waitForConsumerToRegister(kinesisClient: KinesisAsyncClient, consumer: Consumer): Unit = {
    var consumerResponse: DescribeStreamConsumerResponse = null
    do {
      println("Waiting for enhanced consumer to become active")
      Helper.sleep()
      consumerResponse = kinesisClient.describeStreamConsumer(DescribeStreamConsumerRequest
        .builder
        .consumerARN(consumer.consumerARN).build
      ).get
    } while ( {
      consumerResponse.consumerDescription.consumerStatus != ConsumerStatus.ACTIVE
    })
  }

  def registerConsumer(kinesisClient: KinesisAsyncClient): Consumer = {
    val region = sys.env.getOrElse("REGION", "us-east-1")
    val account = sys.env.get("ACCOUNT").mkString
    val registerStreamConsumerRequest = RegisterStreamConsumerRequest
      .builder.consumerName("fan-out-consumer")
      .streamARN(s"arn:aws:kinesis:$region:$account:stream/${Helper.STREAM_NAME}")
      .build

    val response = kinesisClient.registerStreamConsumer(registerStreamConsumerRequest)
    val consumer = response.get.consumer
    println("Registered consumer: " + consumer)

    consumer
  }

  class RecordsProcessor extends SubscribeToShardResponseHandler.Visitor {
    override def visit(event: SubscribeToShardEvent): Unit = {
      for (record <- event.records.asScala) {
        Helper.processRecord(record)
      }
    }
  }

}
