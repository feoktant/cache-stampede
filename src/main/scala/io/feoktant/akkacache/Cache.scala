package io.feoktant.akkacache

import akka.actor.{Actor, Props}
import io.feoktant.akkacache.Cache.GetValueWithBlock

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

class Cache(recomputeValueF: => Future[Int],
            ttl: FiniteDuration)
  extends Actor {

  override def receive: Receive = {
    case GetValueWithBlock(key) =>
      context.child(key) match {
        case Some(worker) =>
          worker forward BlockingCacheWorker.GetValue
        case None =>
          val worker = context.actorOf(BlockingCacheWorker.props(recomputeValueF, ttl), key)
          worker forward BlockingCacheWorker.GetValue
      }
  }

}

object Cache {
  sealed trait CacheProtocol
  case class GetValueWithBlock(key: String) extends CacheProtocol

  def props(recomputeValueF: => Future[Int], ttl: FiniteDuration): Props =
    Props(new Cache(recomputeValueF, ttl))
}
