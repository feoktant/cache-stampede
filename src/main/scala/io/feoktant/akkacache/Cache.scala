package io.feoktant.akkacache

import akka.actor.{Actor, Props}
import io.feoktant.akkacache.Cache.{GetValueRedisWithBlock, GetValueWithBlock}
import scredis.Redis
import scredis.serialization.{Reader, Writer}

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

class Cache[T](redis: Redis,
            recomputeValueF: => Future[T],
            ttl: FiniteDuration)(implicit
                                 reader: Reader[T],
                                 writer: Writer[T])
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

    case GetValueRedisWithBlock(key) =>
      context.child(key) match {
        case Some(worker) =>
          worker forward RedisBlockingCacheWorker.GetValue
        case None =>
          val worker = context.actorOf(RedisBlockingCacheWorker.props(redis, recomputeValueF, ttl), key)
          worker forward RedisBlockingCacheWorker.GetValue
      }
  }

}

object Cache {
  sealed trait CacheProtocol
  case class GetValueWithBlock(key: String) extends CacheProtocol
  case class GetValueRedisWithBlock(key: String) extends CacheProtocol

  def props[T](redis: Redis,
            recomputeValueF: => Future[T],
            ttl: FiniteDuration)(implicit
                                 reader: Reader[T],
                                 writer: Writer[T]): Props =
    Props(new Cache(redis, recomputeValueF, ttl))
}
