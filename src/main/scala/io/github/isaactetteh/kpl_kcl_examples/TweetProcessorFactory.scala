package io.github.isaactetteh.kpl_kcl_examples

import software.amazon.kinesis.processor.{ShardRecordProcessor, ShardRecordProcessorFactory}

class TweetProcessorFactory extends ShardRecordProcessorFactory{
  override def shardRecordProcessor(): ShardRecordProcessor = {
    new TweetsProcessor()
  }
}
