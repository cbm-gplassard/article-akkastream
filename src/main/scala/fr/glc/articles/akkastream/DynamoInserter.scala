package fr.glc.articles.akkastream

import java.time.Instant

import akka.actor.ActorSystem
import akka.stream.ThrottleMode
import akka.stream.alpakka.dynamodb.scaladsl.DynamoDb
import akka.stream.scaladsl.{Sink, Source}
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model.{AttributeValue, BatchWriteItemRequest, PutRequest, WriteRequest}

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

class DynamoInserter(implicit val actorSystem: ActorSystem, implicit val dynamoDb: DynamoDbAsyncClient) {

  def insertItems(tableName: String, count: Int): Future[Int] = {
    Source(1 to count)
      .map(index => Map(
        "pk" -> AttributeValue.builder().s(index.toString).build(),
        "created" -> AttributeValue.builder().s(Instant.now().toString).build()
      ))
      .map(item => WriteRequest.builder().putRequest(PutRequest.builder().item(item.asJava).build()).build())
      .grouped(10)
      .map(requests => BatchWriteItemRequest.builder().requestItems(Map(tableName -> requests.toList.asJava).asJava).build())
      .via(DynamoDb.flow(10))
      .throttle(100, 1 second, 100, _.consumedCapacity().asScala.map(_.writeCapacityUnits().doubleValue()).sum.toInt, ThrottleMode.Shaping)
      .runWith(Sink.fold(0)((count, _) => count + 1))
  }

}
