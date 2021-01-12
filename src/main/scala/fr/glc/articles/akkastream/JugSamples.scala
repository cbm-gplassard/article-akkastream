package fr.glc.articles.akkastream

import akka.actor.ActorSystem
import akka.stream.scaladsl.{Flow, Sink, Source}
import akka.{Done, NotUsed}

import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContext, Future}

object JugSamples extends App {
  implicit val system: ActorSystem = ActorSystem()
  implicit val ec: ExecutionContext = system.dispatcher

  // Source qui produira 100 entiers de 1 à 100
  val source: Source[Int, NotUsed] = Source(1 to 100)

  // Sink qui consomme des Int et produit un Future[Seq[Int]]
  val sink: Sink[Int, Future[Seq[Int]]] = Sink.seq[Int]

  // Flow qui ne garde que les nombres pairs
  val flow: Flow[Int, Int, NotUsed] = Flow[Int]
    .filter(i => i % 2 == 0)


  val source2: Source[Int, NotUsed] = source.via(flow)

  val flow2: Flow[Int, Int, NotUsed] = flow.via(flow)

  val sink2: Sink[Int, NotUsed] = flow.to(sink)

  val elements: Future[Done] = Source(1 to 100)
    .filter(v => v > 50)
    .map(v => v * 2)
    .take(5)
    .throttle(1, 1 second)
    .map(v => v.toString)
    .runForeach(s => println(s))

  val result: Future[Done] = Source.single(Future{List(1, 2, 3, 4)})
    .mapAsyncUnordered(3)(future => future)
    .mapConcat(liste => liste)
    .grouped(2)
    .runForeach(int => println(int))

}
