/*
 * TODO: License goes here!
 */

package sa.com.mobily.location

import org.apache.spark.sql.Row

import sa.com.mobily.parsing.{CsvParser, OpenCsvParser, RowParser}
import sa.com.mobily.poi.{Poi, PoiType}

case class LocationPoiView(
    imsi: String,
    mcc: String,
    name: String,
    poiType: PoiType,
    weight: Double) {

  def fields: Array[String] = Array(imsi, mcc, name, poiType.identifier, weight.toString)
}

object LocationPoiView {

  val MaxWeight = 1

  val Header: Array[String] = Array("imsi", "mcc", "name", "poi-type", "weight")

  def apply(
      location: Location,
      poi: Poi,
      weight: Double = MaxWeight): LocationPoiView =
    LocationPoiView(
      imsi = poi.user.imsi,
      mcc = poi.user.mcc,
      name = location.name,
      poiType = poi.poiType,
      weight = weight)

  implicit val fromCsv = new CsvParser[LocationPoiView] {

    override def lineCsvParser: OpenCsvParser = new OpenCsvParser

    override def fromFields(fields: Array[String]): LocationPoiView = {
      val Array(imsi, mcc, name, poiType, weight) = fields

      LocationPoiView(
        imsi = imsi,
        mcc = mcc,
        name = name,
        poiType = PoiType(poiType),
        weight = weight.toDouble)
    }
  }

  implicit val fromRow = new RowParser[LocationPoiView] {

    override def fromRow(row: Row): LocationPoiView = {
      val Row(imsi, mcc, name, poiTypeRow, weight) = row
      val Row(poiType) = poiTypeRow.asInstanceOf[Row]

      LocationPoiView(
        imsi = imsi.asInstanceOf[String],
        mcc = mcc.asInstanceOf[String],
        poiType = PoiType(poiType.asInstanceOf[String]),
        name = name.asInstanceOf[String],
        weight = weight.asInstanceOf[Double])
    }
  }
}
