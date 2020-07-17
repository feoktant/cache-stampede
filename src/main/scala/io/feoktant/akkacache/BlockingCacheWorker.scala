package io.feoktant.akkacache

import akka.actor.{Actor, Kill, PoisonPill, Props, Stash, Timers}
import akka.pattern.pipe
import io.feoktant.akkacache.BlockingCacheWorker._
import org.slf4j.LoggerFactory

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}

class BlockingCacheWorker(recomputeValueF: => Future[Int],
                          ttl: FiniteDuration)
  extends Actor
    with Stash
    with Timers {

  private val log = LoggerFactory.getLogger(classOf[BlockingCacheWorker])
  implicit val ec: ExecutionContext = context.system.dispatcher

  override def preStart(): Unit = {
    restartTimer(KILL_TIMER, Kill)
    timers.startTimerWithFixedDelay(TTL_TIMER, Expired, ttl)
    recomputeValue()
  }

  override def receive: Receive = notInitialized

  private def notInitialized: Receive = {
    case Initialized(v) =>
      context.become(initialized(v))
      unstashAll()

    case GetValue =>
      log.info("Client blocked")
      restartTimer(KILL_TIMER, PoisonPill)
      stash()
  }

  private def initialized(value: Int): Receive = {
    case GetValue =>
      restartTimer(KILL_TIMER, PoisonPill)
      sender() ! value

    case Expired =>
      log.warn("Cached value for key {} expired, recomputing", self.path.name)
      context.become(notInitialized)
      recomputeValue()
  }

  private def recomputeValue(): Unit =
    recomputeValueF.map(Initialized) pipeTo self

  private def restartTimer(key: String, command: Any): Unit = {
    timers.cancel(key)
    timers.startSingleTimer(key, command, ttl)
  }
}

object BlockingCacheWorker {
  val KILL_TIMER = "kill-timer"
  val TTL_TIMER = "ttl-timer"

  sealed trait Protocol
  case object GetValue extends Protocol
  private case class Initialized(value: Int) extends Protocol
  private case object Expired extends Protocol

  def props(recomputeValueF: => Future[Int], ttl: FiniteDuration): Props =
    Props(new BlockingCacheWorker(recomputeValueF, ttl))

}
