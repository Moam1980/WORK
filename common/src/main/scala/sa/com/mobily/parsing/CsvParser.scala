/*
 * TODO: License goes here!
 */

package sa.com.mobily.parsing

import sa.com.mobily.utils.EdmCoreUtils

import scala.util.{Failure, Success, Try}

case class ParsingError(line: String, error: Throwable)

case class ParsedItem[T](value: Option[T], parsingError: Option[ParsingError] = None) {

  require((value.isDefined && !parsingError.isDefined) || (!value.isDefined && parsingError.isDefined))
}

abstract class CsvParser[T] extends Serializable {

  def lineCsvParser : OpenCsvParser

  def fromFields(fields: Array[String]): T
}

object CsvParser {

  def fromLine[T](line: String)(implicit csvParser: CsvParser[T]): ParsedItem[T] =
    Try(csvParser.fromFields(csvParser.lineCsvParser.parseLine(line))) match {
      case Success(v) => ParsedItem(Some(v))
      case Failure(e) => ParsedItem(value = None, parsingError = Some(ParsingError(line, e)))
    }

  def fromLines[T](lines: Iterator[String])(implicit csvParser: CsvParser[T]): Iterator[ParsedItem[T]] =
    lines.map(line => fromLine(line))
}
