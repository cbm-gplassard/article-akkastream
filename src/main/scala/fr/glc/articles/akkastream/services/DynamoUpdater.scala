package fr.glc.articles.akkastream.services

import java.time.Instant
import java.util.concurrent.atomic.AtomicInteger

import akka.actor.ActorSystem
import akka.stream.Attributes.LogLevels
import akka.stream.alpakka.dynamodb.scaladsl.DynamoDb
import akka.stream.scaladsl.{Keep, Sink, Source}
import akka.stream.{Attributes, KillSwitches, ThrottleMode}
import fr.glc.articles.akkastream.Utils
import fr.glc.articles.akkastream.model.UpdateState
import izanami.Strategy.CacheWithSseStrategy
import izanami.scaladsl.{ConfigClient, IzanamiClient}
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model._

import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters._

class DynamoUpdater(izanami: ConfigClient, implicit val actorSystem: ActorSystem, implicit val dynamoDb: DynamoDbAsyncClient) {

  /**
   * Update all the items of the scan response and returns the number of batchWriteRequests used
   */
  private def updateItems(tableName: String, scanResponse: ScanResponse, baseThrottle: Int)(implicit ec: ExecutionContext): Future[Int] = Source.single(scanResponse)
    .mapConcat(_.items().asScala.toList)
    .map(item => PutRequest.builder().item((item.asScala concat Seq(("updatedAt" -> AttributeValue.builder().s(Instant.now().toString).build()))).asJava).build())
    .map(putRequest => WriteRequest.builder().putRequest(putRequest).build())
    .grouped(10)
    .map(requests => BatchWriteItemRequest.builder().requestItems(Map(tableName -> requests.toList.asJava).asJava).returnConsumedCapacity(ReturnConsumedCapacity.TOTAL).build())
    .via(DynamoDb.flow(10))
    .throttle(baseThrottle, 1 second, baseThrottle, item => item.consumedCapacity().asScala.map(_.capacityUnits().doubleValue()).sum.toInt, ThrottleMode.Shaping)
    .alsoTo(Utils.log1Percent())
    .runWith(Sink.fold(0)((count, _) => count + 1))


  def updateDynamo(tableName: String, previousState: Option[UpdateState], baseThrottle: Int)(implicit ec: ExecutionContext): Future[UpdateState] = {
    println(s"Starting updateDynamo from previousState $previousState with throttle $baseThrottle")
    val startKey = previousState match {
      case Some(updateState) if updateState.lastEvaluatedKey != null => Map("pk" -> AttributeValue.builder().s(updateState.lastEvaluatedKey).build()).asJava
      case _ => null
    }
    val updateParallelism = 1

    val (killSwitch, result) = Source
      .single(ScanRequest.builder().tableName(tableName).exclusiveStartKey(startKey).limit(1000).returnConsumedCapacity(ReturnConsumedCapacity.TOTAL).build())
      .via(DynamoDb.flowPaginated())
      .log("ScanResult", e => (e.consumedCapacity(), e.items().size()))
      .throttle(baseThrottle, 1 second, baseThrottle, item => item.consumedCapacity().readCapacityUnits().toInt, ThrottleMode.Shaping)
      .takeWithin(1 minutes)
      .viaMat(KillSwitches.single)(Keep.right)
      .mapAsync(updateParallelism){ scanResponse =>
        updateItems(tableName, scanResponse, baseThrottle / updateParallelism)
          .map(count => (count, scanResponse))
      }
      .withAttributes(Attributes.logLevels(onElement = LogLevels.Info))
      .scan(previousState.getOrElse(UpdateState.zero))( (previous, response) => previous.update(response._1, response._2))
      .toMat(Sink.last)(Keep.both)
      .run()

    izanami.onConfigChanged("article:GUARDIANS:akkastream:settings") { payload =>
      println("Config changed", payload)
      if ((payload \ "abort").as[Boolean]) {
        println("Aborting")
        killSwitch.abort(new RuntimeException("Aborting"))
      }
      if ((payload \ "stop").as[Boolean]) {
        println("Stopping")
        killSwitch.shutdown()
      }
    }

    result
  }

}
