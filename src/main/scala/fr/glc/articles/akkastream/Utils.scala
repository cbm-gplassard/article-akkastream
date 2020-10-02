package fr.glc.articles.akkastream

import akka.NotUsed
import akka.stream.Attributes
import akka.stream.Attributes.LogLevels
import akka.stream.scaladsl.{Flow, Sink}

object Utils {
  def log1Percent[T](): Sink[T, NotUsed] = Flow[T]
    .filter(_ => Math.random() > 0.99)
    .log("Request")
    .withAttributes(Attributes.logLevels(onElement = LogLevels.Info))
    .to(Sink.ignore)
}
