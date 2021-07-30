package com.hourly.analytics

import cats.Show
import cats.implicits._
import com.hourly.analytics.messaging.Queue
import com.hourly.analytics.model.Visit
import com.hourly.analytics.pipeline.DocumentsPipeline
import com.hourly.analytics.pipeline.DocumentsPipeline.{DocumentSummary, VisitState}
import DocumentsPipeline.{DocumentSummary, VisitState}
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

object Main {

  import Summary._

  def app(
    path: String,
    executionEnvironment: StreamExecutionEnvironment
  ): SingleOutputStreamOperator[DocumentSummary] = {
    val linesStream =
      Queue
        .linesFromFile(path, executionEnvironment)
        .messages

    summarise(linesStream)
  }

  def main(args: Array[String]): Unit = {
    val executionEnvironment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment()
    //    env.enableCheckpointing(10000L)

    val path =
      args.headOption
        .getOrElse(throw new Exception("Please provide a valid input file"))

    Summary
      .showLines(app(path, executionEnvironment))
      .print()

    executionEnvironment.execute()
  }
}

object Summary {
  val summaryImpl: DocumentsPipeline[String, Visit, VisitState, DocumentSummary] = DocumentsPipeline.impl

  def summarise(input: SingleOutputStreamOperator[String]): SingleOutputStreamOperator[DocumentSummary] =
    summaryImpl.decodeSource
      .andThen(summaryImpl.aggregateOnVisit)
      .andThen(summaryImpl.aggregateOnDocumentHourly)
      .apply(input)

  def showLines(summary: SingleOutputStreamOperator[DocumentSummary]): SingleOutputStreamOperator[String] =
    summary
      .map((value: DocumentSummary) => value.show)

  implicit val showDocumentSummary: Show[DocumentSummary] = Show.show(
    s => s"${s.documentId.value},${s.start},${s.end},${s.visits},${s.uniques},${s.engagedTime},${s.completion}"
  )
}
