package fr.glc.articles.akkastream.lambda

import fr.glc.articles.akkastream.model.{ListTaskParam, UpdateState}
import scala.jdk.CollectionConverters._

object ListTasks {
  def handler(param: ListTaskParam): java.util.List[UpdateState] = {
    (0 until param.parallelism)
      .map(index => UpdateState.zero.copy(
        segment = index,
        totalSegments = param.parallelism
      ))
      .toList
      .asJava
  }
}
