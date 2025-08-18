package com.ashtry.flinky.streamdemo

import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction
import tv.tubi.flinky.core.kinesis.{ClientLog, TubiKinesisSource}
import com.tubitv.rpc.analytics.events.RawEvent
import org.apache.flink.api.common.eventtime.WatermarkStrategy

// SERIALIZATION NOTES
/*
  // To just see whatever string data is in the stream (protobuf = gibberish)
  val stringSchema = new SimpleStringSchema()

  // Using flink-json's JsonDeserializationSchema[T] doesn't support case classes out of the box
  // in scala. It should support POJO's, however. TODO: check scala class
  val defaultJsonSchema = new JsonDeserializationSchema[ClientLog](classOf[ClientLog])

  // For deserializing into scala case classes, you must use custom object mappers with
  // DefaultScalaModule work.
  val customJsonSchema = new Serializer.ClientLogSchema()

  // Protobuf Schema can be used in custom deserializer
  val protoSchema = new Serializer.RawEventSchema()

  // Tubi pre-config sources are in TubiKinesisSource
 */


object TestPrintStreamJobStreamApi extends App {

  final val env:StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

  val clientLogs:DataStream[ClientLog] = env.fromSource(TubiKinesisSource.clientLogs.getSource, WatermarkStrategy.noWatermarks(), "print")

  clientLogs
    .map { x => (x.platform, x.version) }
    .returns(classOf[(String,String)])
    .addSink(new PrintSinkFunction[(String,String)]())

  import scalapb.json4s.JsonFormat
  val analytics:DataStream[RawEvent] = env.fromSource(
    TubiKinesisSource.analyticsRaw.getSource(Map(
      "scan.stream.recordpublisher" -> "EFO",
      "scan.stream.efo.consumername" -> "snarfd-test",
      "scan.stream.efo.registration" -> "EAGER",
    )), WatermarkStrategy.noWatermarks(), "print"
  )

  analytics
    .map { x:RawEvent => JsonFormat.toJsonString(x) }
    .addSink(new PrintSinkFunction)

  env.execute(getClass.getName)
}
