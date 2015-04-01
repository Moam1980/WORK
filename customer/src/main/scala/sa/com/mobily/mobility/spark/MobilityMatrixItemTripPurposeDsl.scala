/*
 * TODO: License goes here!
 */

package sa.com.mobily.mobility.spark

import scala.language.implicitConversions

import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

import sa.com.mobily.crm.SubscriberProfilingView
import sa.com.mobily.mobility.{MobilityMatrixItemTripPurpose, TripPurpose}
import sa.com.mobily.parsing.spark.{SparkParser, SparkWriter}

class MobilityMatrixItemTripPurposeRowReader(self: RDD[Row]) {

  def toMobilityMatrixItemTripPurpose: RDD[MobilityMatrixItemTripPurpose] =
    SparkParser.fromRow[MobilityMatrixItemTripPurpose](self)
}

class MobilityMatrixItemTripPurposeWriter(self: RDD[MobilityMatrixItemTripPurpose]) {

  def saveAsParquetFile(path: String): Unit = SparkWriter.saveAsParquetFile[MobilityMatrixItemTripPurpose](self, path)
}

class MobilityMatrixItemTripPurposeFunctions(self: RDD[MobilityMatrixItemTripPurpose]) {

  def filterBySubscribersAndTripPurpose(
      subscribers: RDD[SubscriberProfilingView],
      tripPurpose: TripPurpose): RDD[MobilityMatrixItemTripPurpose] = {
    val susbscribersByImsi = subscribers.keyBy(_.imsi)
    val mmitpByImsi = self.keyBy(_.mobilityMatrixItemParquet.user.imsi)
    susbscribersByImsi.join(mmitpByImsi).map(_._2._2).filter(_.tripPurpose == tripPurpose)
  }
}

trait MobilityMatrixItemTripPurposeDsl {

  implicit def mobilityMatrixItemTripPurposeRowReader(self: RDD[Row]): MobilityMatrixItemTripPurposeRowReader =
    new MobilityMatrixItemTripPurposeRowReader(self)

  implicit def mobilityMatrixItemTripPurposeWriter(
      mobilityMatrixItemTripPurposes: RDD[MobilityMatrixItemTripPurpose]): MobilityMatrixItemTripPurposeWriter =
    new MobilityMatrixItemTripPurposeWriter(mobilityMatrixItemTripPurposes)

  implicit def mobilityMatrixItemTripPurposeFunctions(
      mobilityMatrixItemTripPurpose: RDD[MobilityMatrixItemTripPurpose]): MobilityMatrixItemTripPurposeFunctions =
    new MobilityMatrixItemTripPurposeFunctions(mobilityMatrixItemTripPurpose)
}

object MobilityMatrixItemTripPurposeDsl extends MobilityMatrixItemTripPurposeDsl
