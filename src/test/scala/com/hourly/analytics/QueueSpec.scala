package com.hourly.analytics

import com.hourly.analytics.base.FlinkClusterBase
import com.hourly.analytics.messaging.Queue
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.util
import java.util.Collections
import scala.collection.JavaConverters._

class QueueSpec extends AnyFlatSpec with Matchers with FlinkClusterBase {

  behavior.of("Queue.linesFromFile")

  it should "read lines from File for valid Json lines" in {
    val linesStream = mapSampleSummary
    linesStream should be(
      List(
        """{"messageType":"VisitCreate","visit":{"id":"6a5aedbd-e823-44c3-b834-3c745bd72a64","userId":"7df89947-b3f0-4b5c-92da-e3a28a1ffa96","documentId":"ef8756e5-3443-4ace-801e-60ce9044807e","createdAt":"2015-05-18T23:55:30.254Z"}}""",
        """{"messageType":"VisitCreate","visit":{"id":"af8291fe-84ab-4ccb-976f-c84ab3d6aad2","userId":"1782fd2b-3d0f-4f01-88cc-5fbd03707354","documentId":"ef8756e5-3443-4ace-801e-60ce9044807e","createdAt":"2015-05-18T23:55:40.254Z"}}""",
        """{"messageType":"VisitCreate","visit":{"id":"7b179f4d-1487-4462-8fbc-2d68cc7f1ddb","userId":"1782fd2b-3d0f-4f01-88cc-5fbd03707354","documentId":"ef8756e5-3443-4ace-801e-60ce9044807e","createdAt":"2015-05-18T23:55:49.254Z"}}""",
        """{"messageType":"VisitUpdate","visit":{"id":"7b179f4d-1487-4462-8fbc-2d68cc7f1ddb","engagedTime":300,"completion":0.2,"updatedAt":"2015-05-18T23:56:54.357Z"}}""",
        """{"messageType":"VisitUpdate","visit":{"id":"7b179f4d-1487-4462-8fbc-2d68cc7f1ddb","engagedTime":600,"completion":0.4,"updatedAt":"2015-05-19T00:50:55.357Z"}}""",
        """{"messageType":"VisitUpdate","visit":{"id":"7b179f4d-1487-4462-8fbc-2d68cc7f1ddb","engagedTime":900,"completion":1,"updatedAt":"2015-05-19T00:51:54.357Z"}}""",
        """{"messageType":"VisitCreate","visit":{"id":"b0206c66-a044-4878-b888-97f2f48d502e","userId":"630adcc1-e302-40a6-8af2-d2cd84e71722","documentId":"ef8756e5-3443-4ace-801e-60ce9044807e","createdAt":"2015-05-19T00:55:49.254Z"}}""",
        """{"messageType":"VisitCreate","visit":{"id":"3a98e4f3-49cf-48a1-b63a-3eeaa2443f28","userId":"630adcc1-e302-40a6-8af2-d2cd84e71722","documentId":"b61d8914-560f-4985-8a5f-aa974ad0c7ac","createdAt":"2015-05-19T01:55:50.254Z"}}""",
        """{"messageType":"VisitUpdate","visit":{"id":"3a98e4f3-49cf-48a1-b63a-3eeaa2443f28","engagedTime":1800,"completion":0.2,"updatedAt":"2015-05-19T01:55:51.254Z"}}"""
      )
    )
  }

  private def mapSampleSummary: List[String] = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    StringTestingSink.values.clear()

    Queue
      .linesFromFile("src/test/resources/sample.log", env)
      .messages
      .addSink(new StringTestingSink)

    env.execute()

    StringTestingSink.values.asScala.toList
  }
}

class StringTestingSink extends SinkFunction[String] {

  override def invoke(value: String): Unit =
    StringTestingSink.values.add(value)
}

object StringTestingSink {
  val values: util.List[String] = Collections.synchronizedList(new util.ArrayList())
}
