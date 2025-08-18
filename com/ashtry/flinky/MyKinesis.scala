package com.ashtry.flinky.core.kinesis

import com.tubitv.rpc.analytics.RichEvent
import com.tubitv.rpc.analytics.events.RawEvent
import com.tubitv.rpc.analytics.flat.FlatEvent
import org.apache.flink.api.common.serialization.{DeserializationSchema, SerializationSchema}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.{ConfigOptions, Configuration}
import org.apache.flink.connector.kinesis.sink.{KinesisStreamsSink, PartitionKeyGenerator}
import org.apache.flink.connector.kinesis.source.KinesisStreamsSource
import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants
import tv.tubi.flinky.core.utils.ConfigUtils

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}
import java.util.Properties

case class KinesisStreamConfig[T](
                                   streamName: String,
                                   deserializer: Option[DeserializationSchema[T]] = None,
                                   kinesisConfig: Map[String, String],
                                   serializer: Option[SerializationSchema[T]] = None,
                                   partitionFunc: Option[PartitionKeyGenerator[T]] = None,
                                 ) {

  implicit def confToConfiguration(conf: Map[String, String]): Configuration = {
    val config = new Configuration()
    conf.foreach { case (k, v) => config.set(ConfigOptions.key(k).stringType().noDefaultValue(), v) }
    config
  }

  implicit def confToProperties(conf: Map[String, String]): Properties = {
    val props = new Properties()
    conf.foreach { case (k, v) => props.put(k, v) }
    props
  }

  def getSource(overrideProps: Map[String, String] = Map()): KinesisStreamsSource[T] = {
    val builder = KinesisStreamsSource.builder[T]()
      .setStreamArn(streamName)
      .setSourceConfig(kinesisConfig ++ overrideProps)
      .setDeserializationSchema(deserializer.get)

    builder.build()
  }

  /**
   * this will sync case
   * @return
   */
  def getSink(overrideProps: Map[String, String] = Map()): KinesisStreamsSink[T] = {
    KinesisStreamsSink.builder()
      .setKinesisClientProperties(overrideProps ++ kinesisConfig)
      .setSerializationSchema(serializer.get)
      .setPartitionKeyGenerator(partitionFunc.get)
      .setStreamArn(streamName)
      .setFailOnError(false)
      .setMaxBatchSize(500)
      .setMaxInFlightRequests(50)
      .setMaxBufferedRequests(10000)
      .setMaxBatchSizeInBytes(5 * 1024 * 1024)
      .setMaxTimeInBufferMS(5000)
      .setMaxRecordSizeInBytes(1 * 1024 * 1024)
      .build()
  }
}

object TubiKinesisSink {
  private final val defaultProps: Map[String, String] = {
    Map("aws.region" -> ConfigUtils.config.getString("tubi.region"))
  }

  def kinesisCaseClassSink[T](props: Map[String, String], streamArn: String): KinesisStreamConfig[T] = KinesisStreamConfig[T](streamArn,
    serializer = Some(new SerializationSchema[T] {
      override def serialize(element: T): Array[Byte] = SerializationUtil.serializeToByteArray(element)
    }), kinesisConfig = props ++ defaultProps, partitionFunc = Some(new CustomPartitionKeyGenerator[T]))
}

/**
 * kinesis read class
 */
object TubiKinesisSource {

  def analyticsRaw: KinesisStreamConfig[RawEvent] = createKinesisStreamConfig(rawEventConfig)
  def analyticsRich: KinesisStreamConfig[RichEvent] = createKinesisStreamConfig(richEventConfig)
  def analyticsFlat: KinesisStreamConfig[FlatEvent] = createKinesisStreamConfig(flatEventConfig)

  val rawEventConfig = StreamConfigDetails[RawEvent](
    "tubi.raw-event-stream",
    RawEvent.parseFrom,
    classOf[RawEvent]
  )

  val richEventConfig = StreamConfigDetails[RichEvent](
    "tubi.analytics-rich-stream",
    RichEvent.parseFrom,
    classOf[RichEvent]
  )

  val flatEventConfig = StreamConfigDetails[FlatEvent](
    "tubi.analytics-flat-stream",
    FlatEvent.parseFrom,
    classOf[FlatEvent]
  )

  private final val defaultProps: Map[String, String] = {
    Map("aws.region" -> ConfigUtils.config.getString("tubi.region")) ++ Map(
      ConsumerConfigConstants.RECORD_PUBLISHER_TYPE -> ConsumerConfigConstants.RecordPublisherType.EFO.name(),
      ConsumerConfigConstants.EFO_REGISTRATION_TYPE -> ConsumerConfigConstants.EFORegistrationType.EAGER.name(),
      ConsumerConfigConstants.STREAM_INITIAL_POSITION -> "LATEST"
    )
  }

  /**
   * this can make it easy to read kinesis data very easy, as long as the record use [[SerializationUtil]] to serialization
   * @param deserializer
   * @param props
   * @param streamArn
   * @tparam T
   * @return
   */
   def createKinesisStreamConfig[T](clazz: Class[T], streamArn:String, props: Map[String, String]=Map()): KinesisStreamConfig[T] = {
    KinesisStreamConfig[T](
      streamArn,
      Some(new DeserializationSchema[T] {
        override def deserialize(recordValue: Array[Byte]): T = {
          SerializationUtil.deserializeFromByteArray(recordValue)
        }

        override def isEndOfStream(nextElement: T): Boolean = false

        override def getProducedType: TypeInformation[T] = TypeInformation.of(clazz)

      }),
      defaultProps ++ props
    )
  }

  case class StreamConfigDetails[T](streamNameKeyPath: String, parseFrom: Array[Byte] => T, clazz: Class[T])

  /**
   * this will try to convert the case class combiled from protobuf repo to KinesisStreamConfig
   * @param configDetails
   * @param props
   * @tparam T
   * @return
   */
  private def createKinesisStreamConfig[T](configDetails: StreamConfigDetails[T]): KinesisStreamConfig[T] = {
    KinesisStreamConfig[T](
      ConfigUtils.config.getString(configDetails.streamNameKeyPath),
      Some(new DeserializationSchema[T] {
        override def deserialize(message: Array[Byte]): T = configDetails.parseFrom(message)
        override def isEndOfStream(nextElement: T): Boolean = false
        override def getProducedType: TypeInformation[T] = TypeInformation.of(configDetails.clazz)
      }),
      defaultProps
    )
  }

}

class CustomPartitionKeyGenerator[T] extends PartitionKeyGenerator[T] {
  override def apply(element: T): String = {
    element.hashCode.toString
  }
}

/**
 * to serialize/deserialize case class data to Array[Byte] so that we can sync it to kinesis easily.
 */
object SerializationUtil {
  def serializeToByteArray[T](element: T): Array[Byte] = {
    val byteArrayOutputStream = new ByteArrayOutputStream()
    val objectOutputStream = new ObjectOutputStream(byteArrayOutputStream)
    objectOutputStream.writeObject(element)
    objectOutputStream.close()
    byteArrayOutputStream.toByteArray
  }

  def deserializeFromByteArray[T](data: Array[Byte]): T = {
    val byteArrayInputStream = new ByteArrayInputStream(data)
    val objectInputStream = new ObjectInputStream(byteArrayInputStream)
    val result = objectInputStream.readObject().asInstanceOf[T]
    objectInputStream.close()
    result
  }
}
