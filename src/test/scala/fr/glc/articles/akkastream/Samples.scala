package fr.glc.articles.akkastream

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.{Flow, Sink, Source}
import org.scalatest.matchers.must.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._


class Samples extends AnyWordSpec with Matchers {
  implicit val system: ActorSystem = ActorSystem()
  implicit val ec: ExecutionContext = system.dispatcher

  "Source" should {
    val sink = Sink.seq[Int]

    "have a single element" in {
      val source = Source.single(1)

      Await.result(source.runWith(sink), 1 second) must equal(Seq(1))
    }

    "have a multiple elements" in {
      val source = Source(1 to 5)

      Await.result(source.runWith(sink), 1 second) must equal(Seq(1, 2, 3, 4, 5))
    }

    "have no elements" in {
      val source = Source.empty

      Await.result(source.runWith(sink), 1 second) must equal(Seq.empty)
    }

    "be async" in {
      val source = Source.fromFuture(Future { Thread.sleep(100); 1})

      Await.result(source.runWith(sink), 1 second) must equal(Seq(1))
    }
  }

  "Flow" should {
    val source = Source(1 to 3)
    val sink = Sink.seq[String]

    "transform elements" in {
      val flow = Flow[Int]
        .map(_.toString)

      Await.result(source.via(flow).runWith(sink), 1 second) must equal(Seq("1", "2", "3"))
    }

    "transform 1 element to multiple elements" in {
      val flow = Flow[Int]
        .mapConcat(i => (1 to i).map(_ => i))
        .map(_.toString)

      Await.result(source.via(flow).runWith(sink), 1 second) must equal(Seq("1", "2", "2", "3", "3", "3"))
    }

    "filter elements" in {
      val flow = Flow[Int]
        .filter(_ != 2)
        .map(_.toString)

      Await.result(source.via(flow).runWith(sink), 1 second) must equal(Seq("1", "3"))
    }

    "transform async" in {
      val flow = Flow[Int]
        .mapAsync(1)(i => Future { Thread.sleep(100); i})
        .map(_.toString)

      Await.result(source.via(flow).runWith(sink), 1 second) must equal(Seq("1", "2", "3"))
    }
  }

  "Connect elements" should {
    val sink = Sink.seq[String]

    "source with flow" in {
      val source = Source(1 to 3)
      val flow = Flow[Int].map(_.toString)

      val newSource: Source[String, NotUsed] = source.via(flow)

      Await.result(newSource.runWith(sink), 1 second) must equal(Seq("1", "2", "3"))
    }

  }

}
