package fr.glc.articles.akkastream.model

import software.amazon.awssdk.services.dynamodb.model.{AttributeValue, ScanResponse}

import scala.beans.BeanProperty
import java.time.Instant

case class UpdateState( @BeanProperty var startTime: Instant,
                        @BeanProperty var batchWriteCount: Int,
                        @BeanProperty var itemsCount: Int,
                        @BeanProperty var lastEvaluatedKey: java.util.Map[String, AttributeValue]) {

  def this() {
    this(Instant.now(), 0, 0, null)
  }

  def update(batchWriteCount: Int, scanResponse: ScanResponse): UpdateState = UpdateState(
    this.startTime,
    this.batchWriteCount + batchWriteCount,
    this.itemsCount + scanResponse.items().size(),
    scanResponse.lastEvaluatedKey()
  )
}

object UpdateState {
  val zero: UpdateState = new UpdateState()
}
