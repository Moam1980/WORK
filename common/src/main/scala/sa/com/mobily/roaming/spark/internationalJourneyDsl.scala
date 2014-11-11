/*
 * TODO: License goes here!
 */

package sa.com.mobily.roaming.spark

import scala.language.implicitConversions

import org.apache.spark.rdd.RDD

import sa.com.mobily.parsing.{ParsingError, ParsedItem}
import sa.com.mobily.parsing.spark.{SparkParser,ParsedItemsDsl}
import sa.com.mobily.roaming.InternationalJourney

class InternationalJourneyReader(self: RDD[String]) {

  import ParsedItemsDsl._

  def toParsedInternationalJourney: RDD[ParsedItem[InternationalJourney]] =
    SparkParser.fromCsv[InternationalJourney](self)

  def toInternationalJourney: RDD[InternationalJourney] = toParsedInternationalJourney.values

  def toInternationalJourneyErrors: RDD[ParsingError] = toParsedInternationalJourney.errors
}

trait InternationalJourneyDsl {

  implicit def internationalJourneyReader(csv: RDD[String]): InternationalJourneyReader =
    new InternationalJourneyReader(csv)
}

object InternationalJourneyDsl extends InternationalJourneyDsl with ParsedItemsDsl
