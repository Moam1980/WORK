/*
 * TODO: License goes here!
 */

package sa.com.mobily.ia.spark

import scala.language.implicitConversions

import org.apache.spark.rdd.RDD

import sa.com.mobily.ia.SubscriberIaVolume
import sa.com.mobily.parsing.{ParsingError, ParsedItem}
import sa.com.mobily.parsing.spark.{ParsedItemsDsl, SparkParser}

class SubscriberIaVolumeReader(self: RDD[String]) {

  import ParsedItemsDsl._

  def toParsedSubscriberIaVolume: RDD[ParsedItem[SubscriberIaVolume]] =
    SparkParser.fromCsv[SubscriberIaVolume](self)

  def toSubscriberIaVolume: RDD[SubscriberIaVolume] = toParsedSubscriberIaVolume.values

  def toSubscriberIaVolumeErrors: RDD[ParsingError] = toParsedSubscriberIaVolume.errors
}

trait SubscriberIaVolumeDsl {

  implicit def subscriberVolumeReader(csv: RDD[String]): SubscriberIaVolumeReader =
    new SubscriberIaVolumeReader(csv)
}

object SubscriberIaVolumeDsl extends SubscriberIaVolumeDsl with ParsedItemsDsl
