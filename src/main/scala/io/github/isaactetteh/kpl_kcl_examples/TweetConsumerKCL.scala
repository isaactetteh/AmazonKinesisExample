package io.github.isaactetteh.kpl_kcl_examples

import io.github.isaactetteh.utils.Helper
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient
import software.amazon.kinesis.common.{ConfigsBuilder, KinesisClientUtil}
import software.amazon.kinesis.coordinator.Scheduler

object TweetConsumerKCL {
  def main(args: Array[String]): Unit = {

    val region = Region.US_EAST_1
    val kinesisClient = KinesisClientUtil.createKinesisAsyncClient(
      KinesisAsyncClient.builder.region(region)
    )
    val dynamoClient = DynamoDbAsyncClient.builder.region(region).build
    val cloudWatchClient = CloudWatchAsyncClient.builder.region(region).build
    val config = new ConfigsBuilder(
      Helper.STREAM_NAME,
      "tweet-processor",
      kinesisClient,
      dynamoClient,
      cloudWatchClient,
      "worker-1",
      new TweetProcessorFactory
    )

    val scheduler: Scheduler = new Scheduler(
      config.checkpointConfig(),
      config.coordinatorConfig(),
      config.leaseManagementConfig(),
      config.lifecycleConfig(),
      config.metricsConfig(),
      config.processorConfig(),
      config.retrievalConfig()
    )

    scheduler.run()

  }

}
