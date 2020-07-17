package io.feoktant

import org.slf4j.{Logger, LoggerFactory}
import scredis.Redis
import scredis.serialization.{Reader, Writer}

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.FiniteDuration
import scala.util.Random

trait CachingAlgorithms {

  private val log: Logger = LoggerFactory.getLogger(classOf[CachingAlgorithms])
  private val random = Random

  implicit  def ec: ExecutionContext
  def redis: Redis

  def naiveCache[T](key: String, ttl: FiniteDuration)
                           (recomputeValue: => Future[T])
                           (implicit
                            reader: Reader[T],
                            writer: Writer[T]): Future[T] = {
    for {
      cached <- redis.get[T](key)
      value  <- cached match {
        case Some(v) =>
          log.debug("Cached key {}", key)
          Future.successful(v)

        case None    =>
          log.warn("Cache miss for key {}", key)
          recomputeValue.map { value =>
            redis.pSetEX(key, value, ttl.toMillis) // side effect
            value
          }
      }
    } yield value
  }

  /**
   * Cache stampede algorithm implementation with Redis.
   * Canonical pseudo-code:
   *
   * function x-fetch(key, ttl, beta=1) {
   *   value, delta, expiry ← cache_read(key)
   *   if (!value || time() − delta * beta * log(rand(0,1)) ≥ expiry) {
   *     start ← time()
   *     value ← recompute_value()
   *     delta ← time() – start
   *     cache_write(key, (value, delta), ttl)
   *   }
   *   return value
   * }
   *
   * @see http://cseweb.ucsd.edu/~avattani/papers/cache_stampede.pdf
   * @see https://www.slideshare.net/RedisLabs/redisconf17-internet-archive-preventing-cache-stampede-with-redis-and-xfetch
   */
  def xFetch[T](key: String, ttl: FiniteDuration, beta: Double = 1.0D)
                       (recomputeValue: => Future[T])
                       (implicit
                        redisTuple2GenericLongReader: Reader[(T, Long)],
                        redisTuple2GenericLongWriter: Writer[(T, Long)]): Future[T] = {
    import System.{currentTimeMillis => time}

    def shouldRecompute(delta: Long, expiryTtl: Long): Boolean =
      (delta * beta * Math.log(random.nextDouble())).abs >= expiryTtl

    val cachedF = redis.get[(T, Long)](key)
    val ttlMsF = redis.pTtl(key)

    def recompute(): Future[T] = {
      val start = time()
      recomputeValue.map { value =>
        val delta = time() - start
        redis.pSetEX(key, (value, delta), ttl.toMillis) // side effect
        log.debug("Recompute key {} took {}ms", key, delta)
        value
      }
    }

    for {
      cached    <- cachedF
      expiryTtl <- ttlMsF
      value     <- (cached, expiryTtl) match {
        case (Some((value, delta)), Right(expiryTtl)) =>
          if (shouldRecompute(delta, expiryTtl)) {
            log.info("Should recompute key {}, remaining ttl {}ms", key, expiryTtl)
            recompute() // side effect
          }
          log.debug("Cached key {}", key)
          Future.successful(value)

        case (Some((value, _)), Left(true)) =>
          log.warn("xFetch key {} with no TTL, skipping update", key)
          Future.successful(value)

        case _ =>
          log.warn("Cache miss for key {}", key)
          recompute()
      }
    } yield value
  }

}
