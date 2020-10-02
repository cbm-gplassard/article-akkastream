package fr.glc.articles.akkastream.services

import java.time.Instant
import java.util.concurrent.atomic.AtomicInteger

import akka.actor.ActorSystem
import akka.stream.alpakka.dynamodb.scaladsl.DynamoDb
import akka.stream.scaladsl.{Keep, Sink, Source}
import akka.stream.{KillSwitches, ThrottleMode}
import fr.glc.articles.akkastream.Utils
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
  private def updateItems(tableName: String, scanResponse: ScanResponse, baseThrottle: Int, throttleRatio: AtomicInteger)(implicit ec: ExecutionContext): Future[Int] = Source.single(scanResponse)
    .mapConcat(_.items().asScala.toList)
    .map(item => PutRequest.builder().item((item.asScala concat Seq(("updatedAt" -> AttributeValue.builder().s(Instant.now().toString).build()))).asJava).build())
    .map(putRequest => WriteRequest.builder().putRequest(putRequest).build())
    .grouped(10)
    .map(requests => BatchWriteItemRequest.builder().requestItems(Map(tableName -> requests.toList.asJava).asJava).returnConsumedCapacity(ReturnConsumedCapacity.TOTAL).build())
    .via(DynamoDb.flow(10))
    .throttle(baseThrottle * 100, 1 second, baseThrottle, item => 100 * throttleRatio.get() * item.consumedCapacity().asScala.map(_.capacityUnits().doubleValue()).sum.toInt, ThrottleMode.Shaping)
    .runWith(Sink.fold(0)((count, _) => count + 1))


  def updateDynamo(tableName: String, previousState: Option[UpdateState], baseThrottle: Int)(implicit ec: ExecutionContext): Future[UpdateState] = {
    val config = izanami.configClient(CacheWithSseStrategy(Seq("article.akkastream")))
    val throttleRatio = new AtomicInteger(1)
    val startKey = previousState.map(_.lastEvaluatedKey).orNull
    val updateParralelism = 10

    val (killSwitch, result) = Source
      .single(ScanRequest.builder().tableName(tableName).exclusiveStartKey(startKey).returnConsumedCapacity(ReturnConsumedCapacity.TOTAL).build())
      .via(DynamoDb.flowPaginated())
      .throttle(baseThrottle * 100, 1 second, baseThrottle, item => (100 * throttleRatio.get() * item.consumedCapacity().readCapacityUnits()).toInt, ThrottleMode.Shaping)
      .takeWithin(7 minutes)
      .viaMat(KillSwitches.single)(Keep.right)
      .mapAsync(updateParralelism){ scanResponse =>
        updateItems(tableName, scanResponse, baseThrottle / updateParralelism, throttleRatio)
          .map(count => (count, scanResponse))
      }
      .scan(previousState.getOrElse(UpdateState.zero))( (previous, response) => previous.update(response._1, response._2))
      .alsoTo(Utils.log1Percent())
      .toMat(Sink.last)(Keep.both)
      .run()

    config.onConfigChanged("article:GUARDIANS:article.akkastream.settings") { payload =>
      println("Config changed", payload)
      val throttleValue = (payload \ "value").as[Int]
      throttleRatio.set(throttleValue)
      if ((payload \ "stop").as[Boolean]) {
        println("Aborting")
        killSwitch.abort(new RuntimeException("Shutting down !"))
      }
    }


    result
  }

}
