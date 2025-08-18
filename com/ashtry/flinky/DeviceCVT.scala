package tv.tubi.flinky.realtimefeature

import com.tubitv.rpc.analytics.flat.FlatEvent
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.connector.kinesis.sink.KinesisStreamsSink
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows
import tv.tubi.flinky.core.customizedfunction.CustomizedContinuousProcessingTimeTrigger
import tv.tubi.flinky.core.kinesis.{TubiKinesisSink, TubiKinesisSource}
import tv.tubi.flinky.core.utils.{ConfigUtils, Datadog}
import tv.tubi.flinky.realtimefeature.models.{DeviceCVTInput, DeviceCVTRealtimeFeatureFamily}
import tv.tubi.flinky.realtimefeature.sinks.ScylladbSink
import tv.tubi.flinky.realtimefeature.stateoperators.DeviceSessionWindowNativeAPICVTAggLogic

import java.time.Duration

object DeviceCVT extends App with StreamAbstract with Datadog {
  override val jobName: String = "device_cvt_session_feature_test_5"
  override val checkPointDirectory: String = s"${ConfigUtils.config.getString("tubi.base-checkpoint-path")}/$jobName"
  override val dataPath: String = s"${ConfigUtils.config.getString("tubi.base-data-save-bucket")}/$jobName"
  override val stateChangelogStorage: String = s"${ConfigUtils.config.getString("tubi.base-changelog-save-bucket")}/$jobName"

  private final val streamExecutionEnvironment: StreamExecutionEnvironment = flinkEnv()
  streamExecutionEnvironment.getCheckpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)

  val scylladbSinkTable = "device_cvt_realtime_feature_test"
  ScylladbSink.createTable[DeviceCVTRealtimeFeatureFamily](keyspace, scylladbSinkTable)

  //  see this part to know more about waterMark
  val watermarkAssigner: WatermarkStrategy[FlatEvent] = WatermarkStrategy
    .forBoundedOutOfOrderness[FlatEvent](Duration.ofSeconds(7200))
    .withIdleness(Duration.ofMinutes(1))
    .withTimestampAssigner(new SerializableTimestampAssigner[FlatEvent] {
      override def extractTimestamp(element: FlatEvent, recordTimestamp: Long): Long = element.recvTimestamp
    })

  val deviceCVT: DataStream[DeviceCVTRealtimeFeatureFamily] =   streamExecutionEnvironment.fromSource(
      TubiKinesisSource.analyticsFlat.getSource(),watermarkAssigner,jobName
    )
    .map {
      r => DeviceCVTInput(r.deviceId, r.viewTime.getOrElse(0).toLong, r.recvTimestamp)
    }
    .filter(_.view_time > 0)
    .map { r =>
      val lag = (System.currentTimeMillis() - r.recv_timestamp) / 1000
      Datadog.histogram(s"flink.device_cvt_latency", lag, Seq(): _*)
      r
    }
    .keyBy(_.device_id)
    .window(EventTimeSessionWindows.withGap(Duration.ofDays(180)))
    // this is important, which is like spark update mode, although the
    // window is not finished, but it will output the data, according to the
    // interval we set. another similar is like EventTimeTrigger, which is like
    // append mode.
    .trigger(CustomizedContinuousProcessingTimeTrigger.of(Duration.ofSeconds(5)))
    .aggregate(new DeviceSessionWindowNativeAPICVTAggLogic)
    .map {
      r =>
        Datadog.count("flink.device_cvt_count", 1, Seq(): _*)
        r
    }


  val kinesisSink: KinesisStreamsSink[DeviceCVTRealtimeFeatureFamily] = TubiKinesisSink.kinesisCaseClassSink[DeviceCVTRealtimeFeatureFamily](props = Map(), streamArn = ConfigUtils.config.getString("tubi.flink-test-stream")).getSink()

  deviceCVT.sinkTo(kinesisSink)

  ScylladbSink.sink[DeviceCVTRealtimeFeatureFamily](deviceCVT, s"$keyspace.$scylladbSinkTable", 256)

  // if want low latency, this should small
  streamExecutionEnvironment.enableCheckpointing(60000)

  streamExecutionEnvironment.execute(jobName)

}
