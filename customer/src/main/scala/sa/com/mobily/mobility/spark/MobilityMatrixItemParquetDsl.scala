/*
 * TODO: License goes here!
 */

package sa.com.mobily.mobility.spark

import scala.language.implicitConversions

import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

import sa.com.mobily.location.LocationPoiView
import sa.com.mobily.mobility.{LocationType, MobilityMatrixItemParquet, MobilityMatrixItemTripPurpose}
import sa.com.mobily.parsing.spark.{SparkParser, SparkWriter}

class MobilityMatrixItemParquetRowReader(self: RDD[Row]) {

  def toMobilityMatrixItemParquet: RDD[MobilityMatrixItemParquet] =
    SparkParser.fromRow[MobilityMatrixItemParquet](self)
}

class MobilityMatrixItemParquetWriter(self: RDD[MobilityMatrixItemParquet]) {

  def saveAsParquetFile(path: String): Unit = SparkWriter.saveAsParquetFile[MobilityMatrixItemParquet](self, path)
}

class MobilityMatrixItemParquetFunctions(self: RDD[MobilityMatrixItemParquet]) {

  def toMobilityMatrixItemTripPurpose(locationPoiViews: RDD[LocationPoiView]): RDD[MobilityMatrixItemTripPurpose] = {
    val lpvByUserAndLocation = locationPoiViews.keyBy(lpv => (lpv.imsi, lpv.name))

    val mmipByUserAndOrigin = self.keyBy(mmip => (mmip.user.imsi, mmip.startLocation))

    val mmitpOrigins = mmipByUserAndOrigin.leftOuterJoin(lpvByUserAndLocation).map(mmipLpv =>
      MobilityMatrixItemTripPurpose(
        mobilityMatrixItemParquet = mmipLpv._2._1,
        startLocationType = LocationType(mmipLpv._2._2),
        endLocationType = LocationType(None)))

    val mmitpoByUserAndDestination = mmitpOrigins.keyBy(mmiTripPurposeOrigin =>
      (mmiTripPurposeOrigin.mobilityMatrixItemParquet.user.imsi,
        mmiTripPurposeOrigin.mobilityMatrixItemParquet.endLocation))

    mmitpoByUserAndDestination.leftOuterJoin(lpvByUserAndLocation).map(mmiTripPurposeLpv =>
          mmiTripPurposeLpv._2._1.copy(endLocationType = LocationType(mmiTripPurposeLpv._2._2)))
  }
}

trait MobilityMatrixItemParquetDsl {

  implicit def mobilityMatrixItemParquetRowReader(self: RDD[Row]): MobilityMatrixItemParquetRowReader =
    new MobilityMatrixItemParquetRowReader(self)

  implicit def mobilityMatrixItemParquetWriter(
      mobilityMatrixItemParquets: RDD[MobilityMatrixItemParquet]): MobilityMatrixItemParquetWriter =
    new MobilityMatrixItemParquetWriter(mobilityMatrixItemParquets)

  implicit def mobilityMatrixItemParquetFunctions(
      mobilityMatrixItemParquet: RDD[MobilityMatrixItemParquet]): MobilityMatrixItemParquetFunctions =
    new MobilityMatrixItemParquetFunctions(mobilityMatrixItemParquet)
}

object MobilityMatrixItemParquetDsl extends MobilityMatrixItemParquetDsl
