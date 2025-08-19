package com.ashtry.flinky

import org.apache.flink.streaming.api.scala_
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer
import org.apache.flink.streaming.connectors.kinesis.config.AWSConfigConstants
import org.apache.flink.api.common.serialization.SimpleStringSchema

import java.util.Properties

class KinesisConsumerExample {
    def main(args: Array[String]): Unit = {
      val env =  StreamExecutionEnvironment.getExecutionEnvironment()
      val kinesisConsumerConfig = new Properties()
      kinesisConsumerConfig.setProperty(AWSConfigConstants.AWS_REGION, "us-east-1")
      KinesisConsumerCOnfig.setProperty(AWSConfigConstants.AWS_ACCESS_KEY_ID, "your access key")
      KinesisConsumerConfig.setProperty(AWSConfigConstants.AWS_SECRET_ACCESS_KEY, "your secret key")

      val kinesisSource = new FlinkKinesisConsumer[String](
        "yourkinesisstreamname",
        new SimpleStringSchema(),
        KinesisConsumerConfig
      )

      val inputStream: DataStream[String] = env.addSource(kinesisSource)
      inputStream.print()
      env.execute("kinesis consumer example")
    }
}