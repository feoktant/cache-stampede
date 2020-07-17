package io.feoktant

import java.util.concurrent.TimeUnit

import akka.actor.{ActorRef, ActorSystem}
import akka.pattern.ask
import akka.util.Timeout
import io.feoktant.akkacache.Cache
import scredis.Redis
import scredis.serialization.IntReader

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Random

class DeepThought(val redis: Redis,
                  system: ActorSystem)(implicit val ec: ExecutionContext)
  extends CachingAlgorithms {

  private val random = Random
  private val ttl = 20.seconds

  val cacheActor: ActorRef = system.actorOf(
    Cache.props(this.computeTheAnswerOfLife, ttl), "akka-cache-root"
  )

  /** @return The Answer to the Ultimate Question of Life, the Universe, and Everything */
  def computeTheAnswerOfLife: Future[Int] =
    Future {
      val ms = random.between(3.seconds.toMillis, 6.seconds.toMillis)
      Thread.sleep(ms)
      42
    }

  def cachedTheAnswerOfLife: Future[Int] = {
    implicit val intReader: IntReader.type = IntReader
    naiveCache("naive:the_answer_of_life", ttl) {
      computeTheAnswerOfLife
    }
  }

  def cachedXFetchedTheAnswerOfLife: Future[Int] =
    xFetch("xfetch:the_answer_of_life", ttl, beta = 0.1) {
      computeTheAnswerOfLife
    }

  def recomputeBlockingAkka: Future[Int] = {
    implicit val timeout: Timeout = Timeout(15, TimeUnit.SECONDS)
    ask(cacheActor, Cache.GetValueWithBlock("akka_blocking:the_answer_of_life")).mapTo[Int]
  }

}
