/*
 * TODO: License goes here!
 */

package sa.com.mobily.ia.spark

import scala.language.implicitConversions

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._

import sa.com.mobily.ia.SubscriberIaVolume
import sa.com.mobily.parsing.{ParsedItem, ParsingError}
import sa.com.mobily.parsing.spark.{ParsedItemsDsl, SparkParser, SparkWriter}

class SubscriberIaVolumeReader(self: RDD[String]) {

  import ParsedItemsDsl._

  def toParsedSubscriberIaVolume: RDD[ParsedItem[SubscriberIaVolume]] =
    SparkParser.fromCsv[SubscriberIaVolume](self)

  def toSubscriberIaVolume: RDD[SubscriberIaVolume] = toParsedSubscriberIaVolume.values

  def toSubscriberIaVolumeErrors: RDD[ParsingError] = toParsedSubscriberIaVolume.errors
}

class SubscriberIaVolumeRowReader(self: RDD[Row]) {

  def toSubscriberIaVolume: RDD[SubscriberIaVolume] = SparkParser.fromRow[SubscriberIaVolume](self)
}

class SubscriberIaVolumeWriter(self: RDD[SubscriberIaVolume]) {

  def saveAsParquetFile(path: String): Unit = SparkWriter.saveAsParquetFile[SubscriberIaVolume](self, path)
}

trait SubscriberIaVolumeDsl {

  implicit def subscriberVolumeReader(csv: RDD[String]): SubscriberIaVolumeReader = new SubscriberIaVolumeReader(csv)

  implicit def subscriberIaVolumeRowReader(self: RDD[Row]): SubscriberIaVolumeRowReader =
    new SubscriberIaVolumeRowReader(self)

  implicit def subscriberIaVolumeWriter(subscriberIaVolume: RDD[SubscriberIaVolume]): SubscriberIaVolumeWriter =
    new SubscriberIaVolumeWriter(subscriberIaVolume)
}

object SubscriberIaVolumeDsl extends SubscriberIaVolumeDsl with ParsedItemsDsl
