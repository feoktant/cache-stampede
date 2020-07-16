package io.feoktant

import org.slf4j.{Logger, LoggerFactory}
import scredis.Redis
import scredis.serialization.{Reader, Writer}

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Random

class LongRunningService(redis: Redis)(implicit val ec: ExecutionContext) {

  private val log: Logger = LoggerFactory.getLogger(classOf[LongRunningService])

  private val random = Random

  /** @return The Answer to the Ultimate Question of Life, the Universe, and Everything */
  def computeTheAnswerOfLife: Future[Int] =
    Future {
      Thread.sleep(3.seconds.toMillis)
      42
    }

  def cachedMeaningOfLife: Future[Int] =
    xFetch("meaning_of_Life", 10.seconds) {
      computeTheAnswerOfLife
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
  private def xFetch[T](key: String, ttl: FiniteDuration, beta: Double = 1.0D)
                       (recomputeValue: => Future[T])
                       (implicit
                        redisTuple2GenericLongReader: Reader[(T, Long)],
                        redisTuple2GenericLongWriter: Writer[(T, Long)]): Future[T] = {
    import System.{currentTimeMillis => time}

    val cachedF = redis.get[(T, Long)](key)
    val pTtlF = redis.pTtl(key)

    for {
      cached <- cachedF
      expiry <- pTtlF
      value  <- (cached, expiry) match {
        case (Some((value, delta)), Right(expiry)) if time() - delta * beta * Math.log(random.nextDouble()) < expiry =>
          Future.successful(value)
        case (Some((value, _)), Left(true)) =>
          log.warn("xFetch key {} with no TTL, skipping update", key)
          Future.successful(value)
        case (value, _) =>
          if (value.isDefined) log.info("Recomputing key {}", key)
          else log.warn("Cache miss for key {}", key)

          val start = time()
          for {
            value <- recomputeValue
            delta  = time() - start
            _     <- redis.pSetEX[(T, Long)](key, (value, delta), ttl.toMillis)
          } yield value
      }
    } yield value
  }

}
