package fr.glc.articles.akkastream.model

import scala.beans.BeanProperty

case class ListTaskParam(@BeanProperty var parallelism: Int) {

  def this() {
    this(3)
  }
}
