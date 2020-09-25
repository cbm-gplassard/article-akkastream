package fr.glc.articles.akkastream

import java.time.ZoneId

import akka.actor.ActorSystem
import com.github.matsluni.akkahttpspi.AkkaHttpClient
import izanami.ClientConfig
import izanami.IzanamiBackend.SseBackend
import izanami.scaladsl.IzanamiClient
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient

import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success}

object Main {
  def main(args: Array[String]): Unit = {
    /**
     * Setup all needed dependencies
     */
    implicit val actorSystem: ActorSystem = ActorSystem()
    implicit val ec: ExecutionContext = actorSystem.dispatcher

    val akkaHttpClient = AkkaHttpClient
      .builder()
      .withActorSystem(actorSystem)
      .build()
    val dynamodb: DynamoDbAsyncClient = DynamoDbAsyncClient
      .builder()
      .region(Region.EU_WEST_1)
      .httpClient(akkaHttpClient)
      .build()

    val izanami = IzanamiClient(
      ClientConfig(
        host = "http://localhost:9000",
        clientId = None,
        clientSecret = None,
        backend = SseBackend,
        pageSize = 50,
        zoneId = ZoneId.of("Europe/Paris")
      )
    )
    actorSystem.registerOnTermination(akkaHttpClient.close())
    actorSystem.registerOnTermination(dynamodb.close())

    val dynamoUpdater = new DynamoUpdater(izanami, actorSystem, dynamodb)
    val dynamoInserter = new DynamoInserter()(actorSystem, dynamodb)
    //val result = dynamoUpdater.updateDynamo("akkastream", None)
    val result = dynamoInserter.insertItems("akkastream", 100_000)

    result
      .onComplete {
        case Success(value) =>
          println(s"Success ${value}")
          Thread.sleep(5000)
          actorSystem.terminate()

        case Failure(error) =>
          error.printStackTrace()
          println(s"Error ${error}")
          Thread.sleep(5000)
          actorSystem.terminate()
      }
  }
}
