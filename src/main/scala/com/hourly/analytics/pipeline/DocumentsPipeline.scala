package com.hourly.analytics.pipeline

import com.hourly.analytics.model._
import io.circe.parser.decode
import io.scalaland.chimney.dsl._
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator
import org.apache.flink.streaming.api.functions.windowing.WindowFunction
import org.apache.flink.streaming.api.scala.createTypeInformation
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import com.hourly.analytics.model.DataSourceOperator.StreamOperator

import java.lang
import java.time.Instant
import java.time.temporal.ChronoUnit
import scala.collection.JavaConverters._

/***
  * Pipeline that aggregates the input clicks
  */
trait DocumentsPipeline[T, IN, ACC, OUT] {

  def decodeSource: SingleOutputStreamOperator[T] => SingleOutputStreamOperator[IN]

  def aggregateOnVisit: SingleOutputStreamOperator[IN] => SingleOutputStreamOperator[ACC]

  def aggregateOnDocumentHourly: SingleOutputStreamOperator[ACC] => SingleOutputStreamOperator[OUT]

}

object DocumentsPipeline {
  final case class VisitState(
    visitId: VisitId,
    createdAt: Instant,
    userId: UserId,
    documentId: DocumentId,
    engagedTime: BigDecimal,
    completion: Int
  )

  final case class DocumentSummary(
    documentId: DocumentId,
    start: Instant,
    end: Instant,
    visits: Int,
    uniques: Int,
    engagedTime: BigDecimal,
    completion: Int
  )

  def impl: DocumentsPipeline[String, Visit, VisitState, DocumentSummary] =
    new DocumentsPipeline[String, Visit, VisitState, DocumentSummary] {

      /**
        * Transforms/Decodes the given input
        * @return Stream of Visit events
        */
      override def decodeSource: SingleOutputStreamOperator[String] => SingleOutputStreamOperator[Visit] =
        (input: SingleOutputStreamOperator[String]) =>
          input
            .flatMap[Visit](
              (line: String, out: Collector[Visit]) => {
                decode[Visit](line) match {
                  case Right(visit) =>
                    out.collect(visit)
                  case Left(ex) =>
                    println(
                      s"Failed to decode $line to Visit, reason: ${ex.getMessage}, message is not emitted downstream"
                    )
                }
              },
              createTypeInformation[Visit]
            )
            .withName("visits-stream")

      /**
        * Creates groups for the same VisitId and aggregates the information
        * keeping always the latest VisitUpdated message
        *
        * @return VisitState which holds the information from VisitCreated message and
        *         the most recent values from VisitUpdated
        */
      override def aggregateOnVisit =
        (input: SingleOutputStreamOperator[Visit]) =>
          input
            .assignTimestampsAndWatermarks(
              WatermarkStrategy
                .noWatermarks()
                .withTimestampAssigner(new SerializableTimestampAssigner[Visit] {
                  override def extractTimestamp(element: Visit, recordTimestamp: Long): Long =
                    element match {
                      case e: VisitCreate => e.createdAt.truncatedTo(ChronoUnit.HOURS).toEpochMilli
                      case e: VisitUpdate => e.updatedAt.toEpochMilli
                    }
                })
            )
            .keyBy((value: Visit) => value.id)
            .window(EventTimeSessionWindows.withGap(Time.minutes(59)))
            .apply[VisitState](new WindowFunction[Visit, VisitState, VisitId, TimeWindow] {
              override def apply(
                key: VisitId,
                window: TimeWindow,
                input: lang.Iterable[Visit],
                out: Collector[VisitState]
              ): Unit = {
                // we should have exactly one VisitCreated message, if not then don't emit anything
                // we should have at most N VisitUpdated messages
                val events          = input.asScala.toSeq
                val visitCreatedOpt = events.collectFirst { case e: VisitCreate => e }
                val visitUpdated    = events.collect { case e: VisitUpdate => e }.sorted.reverse

                visitCreatedOpt
                  .foreach {
                    created =>
                      val result = visitUpdated match {
                        case head +: _ => //take completion and engagedTime from the most recent VisitUpdated
                          created
                            .into[VisitState]
                            .withFieldConst(_.visitId, key)
                            .withFieldConst(_.engagedTime, BigDecimal(head.engagedTime))
                            .withFieldConst(_.completion, head.completion.toInt)
                            .transform

                        case Nil =>
                          created
                            .into[VisitState]
                            .withFieldConst(_.visitId, key)
                            .withFieldConst(_.engagedTime, BigDecimal(0))
                            .withFieldConst(_.completion, 0)
                            .transform
                      }

                      out.collect(result)
                  }
              }
            })
            .withName("enriched-stream")

      /**
        * Creates groups for the same documentId on hourly window and aggregates the completion and engagedTime
        *
        * @return DocumentSummary
        */
      override def aggregateOnDocumentHourly =
        (
          input: SingleOutputStreamOperator[VisitState]
        ) =>
          input
            .assignTimestampsAndWatermarks(
              WatermarkStrategy
                .noWatermarks()
                .withTimestampAssigner(new SerializableTimestampAssigner[VisitState] {
                  override def extractTimestamp(element: VisitState, recordTimestamp: Long): Long =
                    element.createdAt.truncatedTo(ChronoUnit.HOURS).toEpochMilli
                })
            )
            .keyBy((value: VisitState) => value.documentId)
            .window(EventTimeSessionWindows.withGap(Time.minutes(59)))
            .apply[DocumentSummary](
              new WindowFunction[VisitState, DocumentSummary, DocumentId, TimeWindow] {
                override def apply(
                  key: DocumentId,
                  window: TimeWindow,
                  input: lang.Iterable[VisitState],
                  out: Collector[DocumentSummary]
                ): Unit = {
                  val collectedData = input.asScala.toSeq

                  out.collect(
                    (
                      DocumentSummary(
                        documentId = key,
                        start = Instant.ofEpochMilli(window.getStart),
                        end = Instant.ofEpochMilli(window.getEnd),
                        visits = collectedData.map(_.visitId).toSet.size,
                        uniques = collectedData.map(_.userId).toSet.size,
                        engagedTime = (collectedData.map(_.engagedTime).sum) / 3600,
                        completion = collectedData.map(_.completion).sum
                      )
                    )
                  )
                }
              }
            )
            .withName("aggregated-stream")
    }
}
