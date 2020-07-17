package io.feoktant

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.{ExceptionHandler, Route => AkkaRoute}
import de.heikoseeberger.akkahttpcirce.FailFastCirceSupport
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.ExecutionContext
import scala.util.control.NonFatal

trait Route extends FailFastCirceSupport {
  private val log: Logger = LoggerFactory.getLogger(classOf[Route])

  implicit def ec: ExecutionContext
  def service: DeepThought

  val route: AkkaRoute =
    AkkaRoute.seal(
      path("non-cached") {
        get {
          complete(service.computeTheAnswerOfLife)
        }
      } ~
      path("cached") {
        get {
          complete(service.cachedTheAnswerOfLife)
        }
      } ~
      path("xfetched-cached") {
        get {
          complete(service.cachedXFetchedTheAnswerOfLife)
        }
      } ~
      path("akka-blocking") {
        get {
          complete(service.recomputeBlockingAkka)
        }
      }
    )

  implicit def myExceptionHandler: ExceptionHandler =
    ExceptionHandler {
      case NonFatal(e) =>
        extractUri { uri =>
          log.error(s"Request to $uri could not be handled normally", e)
          complete(StatusCodes.InternalServerError, "Horrible disaster")
        }
    }
}
