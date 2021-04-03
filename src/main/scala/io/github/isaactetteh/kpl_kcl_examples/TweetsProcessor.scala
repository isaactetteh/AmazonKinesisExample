package io.github.isaactetteh.kpl_kcl_examples

import io.github.isaactetteh.utils.Helper
import software.amazon.kinesis.exceptions.{
  InvalidStateException,
  ShutdownException
}
import software.amazon.kinesis.lifecycle.events._
import software.amazon.kinesis.processor.ShardRecordProcessor

import scala.jdk.CollectionConverters._
class TweetsProcessor extends ShardRecordProcessor {
  override def initialize(initializationInput: InitializationInput): Unit = {}

  override def processRecords(
      processRecordsInput: ProcessRecordsInput
  ): Unit = {
    for (record <- processRecordsInput.records().asScala) {
      println(Helper.processRecord(record))
    }
  }

  override def leaseLost(leaseLostInput: LeaseLostInput): Unit = {}

  override def shardEnded(shardEndedInput: ShardEndedInput): Unit = {
    try {
      shardEndedInput.checkpointer().checkpoint()
    } catch {
      case e @(_:InvalidStateException | _:ShutdownException)=> e.printStackTrace()
      case _: Throwable => println("Unknown Error")
    }
  }

  override def shutdownRequested(
      shutdownRequestedInput: ShutdownRequestedInput
  ): Unit = {
    try {
      shutdownRequestedInput.checkpointer().checkpoint()
    } catch {
      case e @(_:InvalidStateException | _:ShutdownException) => e.printStackTrace()
      case _: Throwable => println("Unknown Error")
    }
  }
}
