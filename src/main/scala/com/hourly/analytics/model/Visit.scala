package com.hourly.analytics.model

import io.circe.generic.extras.semiauto.deriveUnwrappedDecoder
import io.circe.generic.semiauto.deriveDecoder
import io.circe.{Decoder, DecodingFailure}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala.createTypeInformation

import java.time.Instant
import java.util.UUID
import scala.math.Ordering

case class VisitId(value: UUID) extends AnyVal
object VisitId {
  implicit val decoder: Decoder[VisitId] = deriveUnwrappedDecoder
}

case class DocumentId(value: UUID) extends AnyVal
object DocumentId {
  implicit val decoder: Decoder[DocumentId] = deriveUnwrappedDecoder
}

case class UserId(value: UUID) extends AnyVal
object UserId {
  implicit val decoder: Decoder[UserId] = deriveUnwrappedDecoder
}

sealed trait Visit {
  def id: VisitId
}

object Visit {
  implicit val decoder: Decoder[Visit] = Decoder.instance { cursor =>
    cursor.get[String]("messageType").flatMap {
      case "VisitCreate" => cursor.get[VisitCreate]("visit")
      case "VisitUpdate" => cursor.get[VisitUpdate]("visit")
      case invalid       => Left(DecodingFailure(s"Invalid message type received: $invalid", cursor.history))
    }
  }

  implicit val typeInformation: TypeInformation[Visit] = createTypeInformation
}

final case class VisitCreate(
  id: VisitId,
  userId: UserId,
  documentId: DocumentId,
  createdAt: Instant
) extends Visit

object VisitCreate {
  implicit val decoder: Decoder[VisitCreate] = deriveDecoder
}

final case class VisitUpdate(
  id: VisitId,
  engagedTime: Int,
  completion: Double,
  updatedAt: Instant
) extends Visit

object VisitUpdate {
  implicit val decoder: Decoder[VisitUpdate]   = deriveDecoder
  implicit val ordering: Ordering[VisitUpdate] = Ordering.by(_.updatedAt)
}
