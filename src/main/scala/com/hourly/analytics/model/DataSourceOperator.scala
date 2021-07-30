package com.hourly.analytics.model

import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator

object DataSourceOperator {
  implicit class StreamOperator[T](val stream: SingleOutputStreamOperator[T]) extends AnyVal {
    def withName(name: String): SingleOutputStreamOperator[T] = stream.uid(name).name(name)
  }
}
