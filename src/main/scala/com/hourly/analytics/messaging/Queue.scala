package com.hourly.analytics.messaging

import com.hourly.analytics.model.Visit
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import com.hourly.analytics.model.DataSourceOperator.StreamOperator

/**
  * A very general interface for a queue of messages, simply represented
  * as a Stream.
  */
abstract class Queue[A] {
  def messages: SingleOutputStreamOperator[String]
}

object Queue {
  def linesFromFile[T <: Visit](
    path: String,
    executionEnvironment: StreamExecutionEnvironment
  ): Queue[String] =
    new Queue[String] {
      override def messages: SingleOutputStreamOperator[String] =
        executionEnvironment
          .readTextFile(path)
          .withName("input-click-stream")
    }
}
