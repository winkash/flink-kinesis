package com.ashtry.flinky.core.kinesis

import org.apache.flink.streaming.connectors.kinesis.KinesisShardAssigner
import org.apache.flink.streaming.connectors.kinesis.model.StreamShardHandle

/**
 * customized assigner, which will loop the shard ids, and give it to a sub task from 0 to max,
 * and then start another loop to make sure, the shards can be distributed faily.
 * @param shardIds
 */
class ShardAssigner(shardIds:Seq[String]) extends KinesisShardAssigner {

  override def assign(shard: StreamShardHandle, numParallelSubtasks: Int): Int = {

    // Calculate the number of times each int should repeat
    val repeatCount = shardIds.length / numParallelSubtasks
    val extra = shardIds.length % numParallelSubtasks

    // Create a list of integers with the required distribution
    val distributedInts = (0 until  numParallelSubtasks).flatMap { i =>
      if (i < extra) List.fill(repeatCount + 1)(i) else List.fill(repeatCount)(i)
    }

    // Pair the strings from `a` with the distributed integers
    val resultMap = shardIds.zip(distributedInts).toMap

    val subIndex = resultMap.getOrElse(shard.getShard.getShardId, Math.abs(shard.getShard.getShardId.hashCode) % numParallelSubtasks)

    subIndex
  }
}
