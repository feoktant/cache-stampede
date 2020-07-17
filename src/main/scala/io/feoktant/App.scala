package io.feoktant

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import org.slf4j.{Logger, LoggerFactory}
import scredis.Redis

import scala.concurrent.ExecutionContext

object App extends App with Route {
  private val log: Logger = LoggerFactory.getLogger("io.feoktant.App")
  implicit val system: ActorSystem = ActorSystem("App")
  implicit val ec: ExecutionContext = system.dispatcher

  val redisClient = Redis()
  override val service = new LongRunningService(redisClient)

  for {
    _ <- Http().bindAndHandle(route, "0.0.0.0", 8080)
  } yield ()

  system.registerOnTermination(redisClient.quit())
}
