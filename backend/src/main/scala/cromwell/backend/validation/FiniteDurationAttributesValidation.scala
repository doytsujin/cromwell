package cromwell.backend.validation

import cats.syntax.validated._
import common.validation.ErrorOr.{ErrorOr, ShortCircuitingFlatMap}
import wom.types.{WomStringType, WomType}
import wom.values.{WomString, WomValue}
import common.validation.Validation._

import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.util.Try


class FiniteDurationAttributesValidation(attributeName: String) extends RuntimeAttributesValidation[FiniteDuration] {

  override def key: String = attributeName
  override def coercion: Traversable[WomType] = List(WomStringType)

  override protected def validateValue: PartialFunction[WomValue, ErrorOr[FiniteDuration]] = {
    case WomString(value) => Try(Duration.apply(value)).toErrorOr.flatMap {
        case fd: FiniteDuration => fd.validNel
        case other => s"Attribute '$key' must have a finite duration but got '$other'".invalidNel
    }
  }
}
