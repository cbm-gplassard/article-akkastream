package fr.glc.articles.akkastream.model

import java.time.Instant

import software.amazon.awssdk.services.dynamodb.model.ScanResponse

import scala.beans.BeanProperty

case class UpdateState( @BeanProperty var startTime: String,
                        @BeanProperty var batchWriteCount: Int,
                        @BeanProperty var itemsCount: Int,
                        @BeanProperty var lastEvaluatedKey: String,
                        @BeanProperty var finished: Boolean) {

  def this() {
    this(Instant.now().toString, 0, 0, null, false)
  }

  def update(batchWriteCount: Int, scanResponse: ScanResponse): UpdateState = UpdateState(
    this.startTime,
    this.batchWriteCount + batchWriteCount,
    this.itemsCount + scanResponse.items().size(),
    Option(scanResponse.lastEvaluatedKey())
      .flatMap(map => Option(map.get("pk")))
      .map(_.s())
      .orNull,
    this.finished || scanResponse.lastEvaluatedKey().isEmpty
  )
}

object UpdateState {
  val zero: UpdateState = new UpdateState()
}
