package fr.glc.articles.akkastream.model

import software.amazon.awssdk.services.dynamodb.model.{AttributeValue, ScanResponse}

import scala.jdk.CollectionConverters._

case class UpdateState(batchWriteCount: Int, itemsCount: Int, lastEvaluatedKey: Option[Map[String, AttributeValue]]) {
  def update(batchWriteCount: Int, scanResponse: ScanResponse): UpdateState = UpdateState(
    this.batchWriteCount + batchWriteCount,
    this.itemsCount + scanResponse.items().size(),
    Option(scanResponse.lastEvaluatedKey().asScala.toMap)
  )
}

object UpdateState {
  val zero: UpdateState = UpdateState(0, 0, None)
}
