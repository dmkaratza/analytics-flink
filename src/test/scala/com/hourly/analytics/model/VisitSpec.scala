package com.hourly.analytics.model

import io.circe.CursorOp.DownField
import io.circe.DecodingFailure

import java.time.Instant
import java.util.UUID
import io.circe.parser.decode
import org.scalactic.TypeCheckedTripleEquals
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class VisitSpec extends AnyFlatSpec with Matchers with TypeCheckedTripleEquals {

  behavior.of("Visit.decoder")

  it should "throw a decoding failure for a message which is not VisitCreated nor VisitUpdated" in {
    val json: String =
      """
        |{
        |  "messageType": "other",
        |  "visit": {
        |    "id": "82abce83-3892-48ee-9f1b-d34c4746ace7",
        |    "userId": "dc0ad841-0b89-4411-a033-d3f174e8d0ad",
        |    "documentId": "7b2bc74e-f529-4f5d-885b-4377c424211d",
        |    "createdAt": "2015-04-22T11:42:07.602Z"
        |  }
        |}
        |""".stripMargin

    decode[Visit](json) should ===(Left(DecodingFailure(s"Invalid message type received: other", List.empty)))
  }

  it should "throw a decoding failure when a message does not include messageType" in {
    val json: String =
      """
        |{
        |  "visit": {
        |    "id": "82abce83-3892-48ee-9f1b-d34c4746ace7",
        |    "userId": "dc0ad841-0b89-4411-a033-d3f174e8d0ad",
        |    "documentId": "7b2bc74e-f529-4f5d-885b-4377c424211d",
        |    "createdAt": "2015-04-22T11:42:07.602Z"
        |  }
        |}
        |""".stripMargin

    decode[Visit](json) should ===(
      Left(DecodingFailure(s"Attempt to decode value on failed cursor", List(DownField("messageType")))),
    )
  }

  it should "decode a valid VisitCreate" in {
    val json: String =
      """
        |{
        |  "messageType": "VisitCreate",
        |  "visit": {
        |    "id": "82abce83-3892-48ee-9f1b-d34c4746ace7",
        |    "userId": "dc0ad841-0b89-4411-a033-d3f174e8d0ad",
        |    "documentId": "7b2bc74e-f529-4f5d-885b-4377c424211d",
        |    "createdAt": "2015-04-22T11:42:07.602Z"
        |  }
        |}
        |""".stripMargin

    val expected: Visit =
      VisitCreate(
        VisitId(UUID.fromString("82abce83-3892-48ee-9f1b-d34c4746ace7")),
        UserId(UUID.fromString("dc0ad841-0b89-4411-a033-d3f174e8d0ad")),
        DocumentId(UUID.fromString("7b2bc74e-f529-4f5d-885b-4377c424211d")),
        Instant.parse("2015-04-22T11:42:07.602Z"),
      )

    decode[Visit](json) should ===(Right(expected))
  }

  it should "decode a valid VisitUpdate" in {
    val json: String =
      """
        |{
        |  "messageType": "VisitUpdate",
        |  "visit": {
        |    "id": "82abce83-3892-48ee-9f1b-d34c4746ace7",
        |    "engagedTime": 25,
        |    "completion": 0.4,
        |    "updatedAt": "2015-04-22T11:42:35.122Z"
        |  }
        |}
        |""".stripMargin

    val expected: Visit =
      VisitUpdate(
        VisitId(UUID.fromString("82abce83-3892-48ee-9f1b-d34c4746ace7")),
        25,
        0.4,
        Instant.parse("2015-04-22T11:42:35.122Z"),
      )

    decode[Visit](json) should ===(Right(expected))
  }

}
