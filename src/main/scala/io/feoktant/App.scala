package io.feoktant

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import scredis.Redis

import scala.concurrent.ExecutionContext

object App extends App with Route {
  implicit val system: ActorSystem = ActorSystem("CachingExampleApp")
  implicit val ec: ExecutionContext = system.dispatcher

  val redisClient = Redis()
  override val service = new DeepThought(redisClient, system)

  Http().bindAndHandle(route, "0.0.0.0", 8080)
  system.registerOnTermination(redisClient.quit())
}
