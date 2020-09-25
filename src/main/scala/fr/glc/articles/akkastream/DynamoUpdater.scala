package fr.glc.articles.akkastream

import java.time.Instant
import java.util.concurrent.atomic.AtomicInteger

import akka.actor.ActorSystem
import akka.stream.KillSwitches
import akka.stream.alpakka.dynamodb.scaladsl.DynamoDb
import akka.stream.scaladsl.{Keep, Sink, Source}
import fr.glc.articles.akkastream.model.UpdateState
import izanami.Strategy.CacheWithSseStrategy
import izanami.scaladsl.IzanamiClient
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model._

import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters._

class DynamoUpdater(izanami: IzanamiClient, implicit val actorSystem: ActorSystem, implicit val dynamoDb: DynamoDbAsyncClient) {

  /**
   * Update all the items of the scan response and returns the number of batchWriteRequests used
   */
  private def updateItems(tableName: String, scanResponse: ScanResponse)(implicit ec: ExecutionContext): Future[Int] = Source.single(scanResponse)
    .mapConcat(_.items().asScala.toList)
    .map(item => PutRequest.builder().item((item.asScala concat Seq(("updatedAt" -> AttributeValue.builder().s(Instant.now().toString).build()))).asJava).build())
    .map(putRequest => WriteRequest.builder().putRequest(putRequest).build())
    .grouped(10)
    .map(requests => BatchWriteItemRequest.builder().requestItems(Map(tableName -> requests.toList.asJava).asJava).build())
    .via(DynamoDb.flow(10))
    .runWith(Sink.fold(0)((count, _) => count + 1))


  def updateDynamo(tableName: String, previousState: Option[UpdateState])(implicit ec: ExecutionContext): Future[UpdateState] = {
    val config = izanami.configClient(CacheWithSseStrategy(Seq("article.akkastream")))
    val throttleRatio = new AtomicInteger(1)
    val startKey = previousState.flatMap(_.lastEvaluatedKey).map(_.asJava).orNull

    val (killSwitch, result) = Source
      .single(ScanRequest.builder().tableName(tableName).exclusiveStartKey(startKey).returnConsumedCapacity(ReturnConsumedCapacity.TOTAL).limit(10).build())
      .via(DynamoDb.flowPaginated())
      .throttle(100 * 100, 1 second, item => (100 * throttleRatio.get() * item.consumedCapacity().readCapacityUnits()).toInt)
      .mapAsync(10)(scanResponse => updateItems(tableName, scanResponse).map(count => (count, scanResponse)))
      .viaMat(KillSwitches.single)(Keep.right)
      .toMat(Sink.fold(previousState.getOrElse(UpdateState.zero)) { (previous, response) => previous.update(response._1, response._2) })(Keep.both)
      .run()

    config.onConfigChanged("article.akkastream.enable") { payload =>
      println("Enable config changed", payload)
      if ((payload \ "stop").as[Boolean]) {
        println("shutting down")
        killSwitch.shutdown()
      }
    }

    config.onConfigChanged("article.akkastream.throttle") { payload =>
      println("Throttle config changed", payload)
      val value = (payload \ "value").as[Int]
      throttleRatio.set(value)
    }

    result
  }

}
