package io.feoktant.akkacache

import akka.actor.{Actor, Props, Stash}
import akka.pattern.pipe
import org.slf4j.LoggerFactory
import scredis.Redis
import scredis.serialization.{Reader, Writer}

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}

class RedisBlockingCacheWorker[T](redis: Redis,
                                  recomputeValueF: => Future[T],
                                  ttl: FiniteDuration
                                  )(implicit
                                   reader: Reader[T],
                                   writer: Writer[T]) extends Actor with Stash {

  import RedisBlockingCacheWorker._

  implicit val ec: ExecutionContext = context.system.dispatcher

  private val log = LoggerFactory.getLogger("io.feoktant.akkacache.RedisBlockingCacheWorker")
  private val key = self.path.name

  override def receive: Receive = notInitialized

  private def notInitialized: Receive = {
    case GetValue =>
      log.info("Blocked")
      recomputeValueF.map(Initialized(_)) pipeTo self
      context.become(initializing)
      stash()

    case CacheMiss =>
      self forward GetValue
  }

  private def initializing: Receive = {
    case GetValue =>
      log.info("Blocked")
      stash()

    case Initialized(value: T) =>
      redis.pSetEX[T](key, value, ttl.toMillis)
      context.become(initialized)
      unstashAll()

    case CacheMiss =>
      self forward GetValue
  }

  private def initialized: Receive = {
    case GetValue =>
      redis.get[T](key).map {
        case Some(value) => CachedValue(value)
        case None => CacheMiss
      }.pipeTo(self)(sender())

    case CachedValue(value) =>
      sender() ! value

    case CacheMiss =>
      log.warn("Expired")
      context.become(notInitialized)
      self forward GetValue
  }

}

object RedisBlockingCacheWorker {

  sealed trait Protocol
  case object GetValue extends Protocol
  private case class Initialized[T](value: T) extends Protocol
  private case class CachedValue[T](value: T) extends Protocol
  private case object CacheMiss extends Protocol

  def props[T](redis: Redis, recomputeValueF: => Future[T], ttl: FiniteDuration)(implicit
                                                                                reader: Reader[T],
                                                                                writer: Writer[T]): Props =
    Props(new RedisBlockingCacheWorker(redis, recomputeValueF, ttl))
}

