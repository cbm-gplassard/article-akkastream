package fr.glc.articles.akkastream.services

import java.time.Instant

import akka.actor.ActorSystem
import akka.stream.ThrottleMode
import akka.stream.alpakka.dynamodb.scaladsl.DynamoDb
import akka.stream.scaladsl.{Sink, Source}
import fr.glc.articles.akkastream.Utils
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model._

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters._

class DynamoInserter(implicit val actorSystem: ActorSystem, implicit val dynamoDb: DynamoDbAsyncClient) {


  def insertItems(tableName: String, count: Int, throttle: Int)(implicit ec: ExecutionContext): Future[Int] = {
    Source(1 to count)
      .map(index => Map(
        "pk" -> AttributeValue.builder().s(index.toString).build(),
        "created" -> AttributeValue.builder().s(Instant.now().toString).build()
      ))
      .map(item => WriteRequest.builder().putRequest(PutRequest.builder().item(item.asJava).build()).build())
      .grouped(10)
      .map(requests => BatchWriteItemRequest.builder().requestItems(Map(tableName -> requests.toList.asJava).asJava).returnConsumedCapacity(ReturnConsumedCapacity.TOTAL).build())
      .via(DynamoDb.flow(10))
      .throttle(throttle, 1 second, throttle, _.consumedCapacity().asScala.map(_.capacityUnits().doubleValue()).sum.toInt, ThrottleMode.Shaping)
      .scan(0)((count, _) => count + 1)
      .alsoTo(Utils.log1Percent())
      .runWith(Sink.lastOption)
      .map(_.getOrElse(0))
  }

}
