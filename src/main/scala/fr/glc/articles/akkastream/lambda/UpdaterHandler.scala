package fr.glc.articles.akkastream.lambda

import java.time.{Duration, Instant, ZoneId}

import akka.actor.ActorSystem
import com.github.matsluni.akkahttpspi.AkkaHttpClient
import fr.glc.articles.akkastream.model.UpdateState
import fr.glc.articles.akkastream.services.DynamoUpdater
import izanami.ClientConfig
import izanami.IzanamiBackend.SseBackend
import izanami.Strategy.CacheWithSseStrategy
import izanami.scaladsl.IzanamiClient
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext}
import scala.util.{Failure, Success, Try}

object UpdaterHandler {
  private implicit val actorSystem: ActorSystem = ActorSystem()
  private implicit val ec: ExecutionContext = ExecutionContext.global

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
  private val izanamiConfig = izanami.configClient(CacheWithSseStrategy(Seq("article")))
  actorSystem.registerOnTermination(akkaHttpClient.close())
  actorSystem.registerOnTermination(dynamodb.close())

  private val dynamoUpdater = new DynamoUpdater(izanamiConfig, actorSystem, dynamodb)


  def handler(state: UpdateState): UpdateState = {
    val start = Instant.now()
    println(s"Start $state")

    val throttle = Try(
      Await.result(izanamiConfig.config("article:GUARDIANS:akkastream:settings").map(json => (json \ "throttle").as[Int]), 5 second)
    ) match {
      case Success(value) => value
      case Failure(error) =>
        println("Could not retrieve throttle from izanami, using default value", error)
        50
    }

    val result = dynamoUpdater.updateDynamo(System.getenv("TABLE_NAME"), Option(state), throttle)
    val finalResult = Await.result(result, 8 minutes)
    val d = Duration.between(start, Instant.now())
    println(s"From $state to $finalResult in $d")
    finalResult
  }
}
