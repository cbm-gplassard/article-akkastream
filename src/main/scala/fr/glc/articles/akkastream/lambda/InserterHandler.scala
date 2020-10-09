package fr.glc.articles.akkastream.lambda

import java.time.{Duration, Instant}

import akka.actor.ActorSystem
import com.github.matsluni.akkahttpspi.AkkaHttpClient
import fr.glc.articles.akkastream.services.DynamoInserter
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext}

object InserterHandler {
  private implicit val actorSystem: ActorSystem = ActorSystem()
  private implicit val ec: ExecutionContext = actorSystem.dispatcher

  private val akkaHttpClient = AkkaHttpClient.builder().withActorSystem(actorSystem).build()
  private val dynamodb: DynamoDbAsyncClient = DynamoDbAsyncClient.builder().region(Region.EU_WEST_1).httpClient(akkaHttpClient).build()
  actorSystem.registerOnTermination(akkaHttpClient.close())
  actorSystem.registerOnTermination(dynamodb.close())

  private val dynamoInserter = new DynamoInserter()(actorSystem, dynamodb)

  def handler(): String = {
    val start = Instant.now()
    val result = dynamoInserter.insertItems(System.getenv("TABLE_NAME"), 30_000, 100)
    val finalResult = Await.result(result, 8 minutes)
    val d = Duration.between(start, Instant.now())
    s"Result $finalResult in $d"
  }
}
