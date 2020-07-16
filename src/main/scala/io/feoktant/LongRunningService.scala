package io.feoktant

import org.slf4j.{Logger, LoggerFactory}
import scredis.Redis
import scredis.serialization.{Reader, UTF8StringWriter, Writer}

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Random

class LongRunningService(redis: Redis)(implicit val ec: ExecutionContext) {

  private val log: Logger = LoggerFactory.getLogger(classOf[LongRunningService])

  private val random = Random

  def computeMeaningOfLife: Future[Int] =
    Future {
      Thread.sleep(3.seconds.toMillis)
      42
    }

  def cachedMeaningOfLife: Future[Int] =
    xFetch("meaning_of_Life", 10.seconds)


  /**
   * Cache stampede algorithm implementation on Redis:
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
   */
  private def xFetch(key: String, ttl: FiniteDuration, beta: Int = 1): Future[Int] = {
    import LongRunningService._
    import System.{currentTimeMillis => time}

    val getKey = redis.get[(Int, Long)](key)
    val getTtl = redis.pTtl(key)

    for {
      cached <- getKey
      expiry <- getTtl
      value  <- (cached, expiry) match {
        case (Some((value, delta)), Right(expiry)) if time() - delta * beta * Math.log(random.nextDouble()) < expiry =>
          Future.successful(value)
        case (Some((value, _)), Left(true)) =>
          Future.successful(value)
        case _ =>
          log.info("Cache miss for key {}, recalculating", key)
          val start = time()
          for {
            value <- computeMeaningOfLife
            delta  = time() - start
            _     <- redis.pSetEX[(Int, Long)](key, (value, delta), ttl.toMillis)
          } yield value
      }
    } yield value
  }

  //    for {
  //      cached         <- getKey
  //      Right(expiry)  <- redis.pTtl(key)
  //      value          <- cached match {
  //        case Some((value, delta)) if time() - delta * beta * Math.log(random.nextDouble()) < expiry =>
  //          Future.successful(value)
  //        case _ =>
  //          val start = time()
  //          for {
  //            value <- service.getMeaningOfLife
  //            delta  = time() - start
  //            _     <- redis.pSetEX[(Int, Long)](key, (value, delta), ttl.toMillis)
  //          } yield value
  //      }
  //    } yield value
}

object LongRunningService {

  implicit object Tuple2Writer extends Writer[(Int, Long)] with Reader[(Int, Long)] {

    override def writeImpl(t: (Int, Long)): Array[Byte] =
      UTF8StringWriter.write(s"${t._1}:${t._2}")

    override protected def readImpl(bytes: Array[Byte]): (Int, Long) = {
      val t = scredis.serialization.UTF8StringReader.read(bytes).partition(_ == ':')
      (t._1.toInt, t._2.toLong)
    }
  }

}
