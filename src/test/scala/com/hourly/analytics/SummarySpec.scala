package com.hourly.analytics

import com.hourly.analytics.base.FlinkClusterBase
import com.hourly.analytics.model.DocumentId
import com.hourly.analytics.pipeline.DocumentsPipeline.DocumentSummary
import com.hourly.analytics.pipeline.DocumentsPipeline.DocumentSummary
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.Instant
import java.util
import java.util.{Collections, UUID}
import scala.collection.JavaConverters._

class SummarySpec extends AnyFlatSpec with Matchers with FlinkClusterBase {

  val elevenPm = Instant.parse("2015-05-18T23:00:00Z")
  val midnight = Instant.parse("2015-05-19T00:00:00Z")
  val oneAm    = Instant.parse("2015-05-19T01:00:00Z")
  val twoAm    = Instant.parse("2015-05-19T02:00:00Z")

  behavior.of("Summary.summarise")

  /**
    * The Flink application program seems to consider an inclusive endTime when performing the windowing function.
    * However the application respects the aggregation of the documents into almost hourly timeseries (by 59 minutes)
    */
  it should "summarise uniques" in {
    val uniques = mapSampleSummaryFromFile(item => (item.documentId, item.start, item.end, item.uniques))
    uniques should be(
      List(
        (docId("ef8756e5-3443-4ace-801e-60ce9044807e"), elevenPm, midnight, 2),
        (docId("ef8756e5-3443-4ace-801e-60ce9044807e"), midnight, oneAm, 1),
        (docId("b61d8914-560f-4985-8a5f-aa974ad0c7ac"), oneAm, twoAm, 1)
      ).map(x => x.copy(_3 = x._3.minusSeconds(60)))
    )
  }

  it should "summarise visits" in {
    val visits = mapSampleSummaryFromFile(item => (item.documentId, item.start, item.end, item.visits))
    visits should be(
      List(
        (docId("ef8756e5-3443-4ace-801e-60ce9044807e"), elevenPm, midnight, 3),
        (docId("ef8756e5-3443-4ace-801e-60ce9044807e"), midnight, oneAm, 1),
        (docId("b61d8914-560f-4985-8a5f-aa974ad0c7ac"), oneAm, twoAm, 1)
      ).map(x => x.copy(_3 = x._3.minusSeconds(60)))
    )
  }

  it should "summarise engaged time" in {
    val visits = mapSampleSummaryFromFile(item => (item.documentId, item.start, item.end, item.engagedTime))
    visits should be(
      List(
        (docId("ef8756e5-3443-4ace-801e-60ce9044807e"), elevenPm, midnight, BigDecimal(0.25)),
        (docId("ef8756e5-3443-4ace-801e-60ce9044807e"), midnight, oneAm, BigDecimal(0)),
        (docId("b61d8914-560f-4985-8a5f-aa974ad0c7ac"), oneAm, twoAm, BigDecimal(0.5))
      ).map(x => x.copy(_3 = x._3.minusSeconds(60)))
    )
  }

  it should "summarise completions" in {
    val visits = mapSampleSummaryFromFile(item => (item.documentId, item.start, item.end, item.completion))
    visits should be(
      List(
        (docId("ef8756e5-3443-4ace-801e-60ce9044807e"), elevenPm, midnight, 1),
        (docId("ef8756e5-3443-4ace-801e-60ce9044807e"), midnight, oneAm, 0),
        (docId("b61d8914-560f-4985-8a5f-aa974ad0c7ac"), oneAm, twoAm, 0)
      ).map(x => x.copy(_3 = x._3.minusSeconds(60)))
    )
  }

  it should "not emit an event downstream when it is decoded unsuccessfully" in {
    val visits = mapSampleSummaryFromElements(
      item => (item)
    )(
      """{"messageType":"OTHER","visit":{"id":"7b179f4d-1487-4462-8fbc-2d68cc7f1ddb","engagedTime":300,"completion":0.2,"updatedAt":"2015-05-18T23:56:54.357Z"}}"""
    )

    visits should ===(List.empty)
  }

  it should "not emit an orphan VisitUpdate message" in {
    val visits = mapSampleSummaryFromElements(
      item => (item.documentId, item.start)
    )(
      """{"messageType":"VisitUpdate","visit":{"id":"7b179f4d-1487-4462-8fbc-2d68cc7f1ddb","engagedTime":300,"completion":0.2,"updatedAt":"2015-05-18T23:56:54.357Z"}}""",
      """{"messageType":"VisitCreate","visit":{"id":"b0206c66-a044-4878-b888-97f2f48d502e","userId":"630adcc1-e302-40a6-8af2-d2cd84e71722","documentId":"ef8756e5-3443-4ace-801e-60ce9044807e","createdAt":"2015-05-19T00:55:49.254Z"}}"""
    )

    visits should ===(
      List(
        (docId("ef8756e5-3443-4ace-801e-60ce9044807e"), midnight)
      )
    )
  }

  private def mapSampleSummaryFromFile[T](fn: DocumentSummary => T): List[T] = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    SummaryTestingSink.values.clear()

    Summary
      .summarise(env.readTextFile("src/test/resources/sample.log"))
      .addSink(new SummaryTestingSink)

    env.execute()

    SummaryTestingSink.values.asScala.map(fn).toList
  }

  private def mapSampleSummaryFromElements[T](fn: DocumentSummary => T)(in: String*): List[T] = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    SummaryTestingSink.values.clear()

    Summary
      .summarise(env.fromElements(in: _*))
      .addSink(new SummaryTestingSink)

    env.execute()

    SummaryTestingSink.values.asScala.map(fn).toList
  }

  private def docId(uuid: String): DocumentId = DocumentId(UUID.fromString(uuid))
}

class SummaryTestingSink extends SinkFunction[DocumentSummary] {

  override def invoke(value: DocumentSummary): Unit =
    SummaryTestingSink.values.add(value)
}

object SummaryTestingSink {
  val values: util.List[DocumentSummary] = Collections.synchronizedList(new util.ArrayList())
}
