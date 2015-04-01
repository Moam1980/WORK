/*
 * TODO: License goes here!
 */

package sa.com.mobily.mobility

import org.apache.spark.sql.catalyst.expressions.Row

import sa.com.mobily.location.LocationPoiView
import sa.com.mobily.parsing.RowParser
import sa.com.mobily.poi.{Home, Other, PoiType, Work}

case class LocationType(locationType: PoiType, weight: Double) {

  def fields: Array[String] = Array(locationType.identifier, weight.toString)
}

object LocationType {

  val DefaultWeight = 1D

  def apply(locationPoiView: Option[LocationPoiView]): LocationType =
    locationPoiView.map(lpv => LocationType(lpv.poiType, lpv.weight)).getOrElse(
      LocationType(locationType = Other, weight = DefaultWeight))

  implicit val fromRow = new RowParser[LocationType] {

    override def fromRow(row: Row): LocationType = {
      val Row(locationTypeIdentifierRow, weight) = row
      val Row(locationTypeIdentifier) = locationTypeIdentifierRow.asInstanceOf[Row]

      LocationType(
        locationType = PoiType(locationTypeIdentifier.asInstanceOf[String]),
        weight = weight.asInstanceOf[Double])
    }
  }
}

case class MobilityMatrixItemTripPurpose(
    mobilityMatrixItemParquet: MobilityMatrixItemParquet,
    startLocationType: LocationType,
    endLocationType: LocationType) {

  def weight: Double =
    ((mobilityMatrixItemParquet.origWeight * startLocationType.weight) +
      (mobilityMatrixItemParquet.destWeight * endLocationType.weight)) / 2

  def tripPurpose: TripPurpose = (startLocationType.locationType, endLocationType.locationType) match {
    case (Home, Work) => HomeToWork
    case (Work, Home) => HomeToWork
    case (Home, Other) => HomeToOther
    case (Other, Home) => HomeToOther
    case _ => NonHomeBased
  }
}

object MobilityMatrixItemTripPurpose {

  implicit val fromRow = new RowParser[MobilityMatrixItemTripPurpose] {

    override def fromRow(row: Row): MobilityMatrixItemTripPurpose = {
      val Row(mobilityMatrixItemParquet, startLocationType, endLocationType) = row

      MobilityMatrixItemTripPurpose(
        mobilityMatrixItemParquet =
          MobilityMatrixItemParquet.fromRow.fromRow(mobilityMatrixItemParquet.asInstanceOf[Row]),
        startLocationType = LocationType.fromRow.fromRow(startLocationType.asInstanceOf[Row]),
        endLocationType = LocationType.fromRow.fromRow(endLocationType.asInstanceOf[Row]))
    }
  }
}
