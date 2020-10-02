package fr.glc.articles.akkastream.lambda

import java.time.{Duration, Instant, ZoneId}

import akka.actor.ActorSystem
import com.github.matsluni.akkahttpspi.AkkaHttpClient
import fr.glc.articles.akkastream.model.UpdateState
import fr.glc.articles.akkastream.services.DynamoUpdater
import izanami.ClientConfig
import izanami.IzanamiBackend.SseBackend
import izanami.scaladsl.IzanamiClient
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext}

object UpdaterHandler {
  private implicit val actorSystem: ActorSystem = ActorSystem()
  private implicit val ec: ExecutionContext = actorSystem.dispatcher

  private val akkaHttpClient = AkkaHttpClient.builder().withActorSystem(actorSystem).build()
  private val dynamodb: DynamoDbAsyncClient = DynamoDbAsyncClient.builder().region(Region.EU_WEST_1).httpClient(akkaHttpClient).build()
  private val izanami = IzanamiClient(
    ClientConfig(
      host = System.getenv("IZANAMI_HOST"),
      clientId = Some(System.getenv("IZANAMI_ID")),
      clientSecret = Some(System.getenv("IZANAMI_SECRET")),
      backend = SseBackend,
      pageSize = 50,
      zoneId = ZoneId.of("Europe/Paris")
    )
  )
  actorSystem.registerOnTermination(akkaHttpClient.close())
  actorSystem.registerOnTermination(dynamodb.close())

  private val dynamoUpdater = new DynamoUpdater(izanami, actorSystem, dynamodb)


  def handler(state: UpdateState): UpdateState = {
    val start = Instant.now()
    val result = dynamoUpdater.updateDynamo(System.getenv("TABLE_NAME"), Option(state), 100)
    val finalResult = Await.result(result, 8 minutes)
    val d = Duration.between(start, Instant.now())
    println(s"From $state to $finalResult in $d")
    finalResult
  }
}
