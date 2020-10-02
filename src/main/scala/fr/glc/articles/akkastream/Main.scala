package fr.glc.articles.akkastream

import java.time.{Duration, Instant, ZoneId}

import akka.actor.ActorSystem
import com.github.matsluni.akkahttpspi.AkkaHttpClient
import fr.glc.articles.akkastream.services.DynamoUpdater
import fr.glc.articles.akkastream.services.DynamoInserter
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

    val start = Instant.now()

    //val previousState = Some(UpdateState(180, 81000, Some(Map("pk" -> AttributeValue.builder().s("50589").build()))))
    val previousState = None
    val result = dynamoUpdater.updateDynamo("akkastream", previousState, 100)
    //val result = dynamoInserter.insertItems("akkastream", 100_000, 1_000)

    result
      .onComplete {
        case Success(value) =>
          val duration = Duration.between(start, Instant.now())
          println(s"Success $value in $duration")
          Thread.sleep(5000)
          actorSystem.terminate()

        case Failure(error) =>
          error.printStackTrace()
          println(s"Error $error")
          Thread.sleep(5000)
          actorSystem.terminate()
      }
  }
}
