/*
 * TODO: License goes here!
 */

package sa.com.mobily.location

import org.apache.spark.sql.Row

import sa.com.mobily.parsing.{CsvParser, OpenCsvParser, RowParser}
import sa.com.mobily.poi.{Poi, PoiType}

case class LocationPoiView(imsi: String, mcc: String, name: String, poiType: PoiType) {

  def fields: Array[String] = Array(imsi, mcc, name, poiType.identifier)
}

object LocationPoiView {

  def apply(location: Location, poi: Poi): LocationPoiView =
    LocationPoiView(poi.user.imsi, poi.user.mcc, location.name, poi.poiType)

  val Header: Array[String] = Array("imsi", "mcc", "name", "poi-type")

  implicit val fromCsv = new CsvParser[LocationPoiView] {

    override def lineCsvParser: OpenCsvParser = new OpenCsvParser

    override def fromFields(fields: Array[String]): LocationPoiView = {
      val Array(imsi, mcc, name, poiType) = fields

      LocationPoiView(imsi = imsi, mcc = mcc, name = name, poiType = PoiType(poiType))
    }
  }

  implicit val fromRow = new RowParser[LocationPoiView] {

    override def fromRow(row: Row): LocationPoiView = {
      val Row(imsi, mcc, name, poiTypeRow) = row
      val Row(poiType) = poiTypeRow.asInstanceOf[Row]

      LocationPoiView(
        imsi = imsi.asInstanceOf[String],
        mcc = mcc.asInstanceOf[String],
        poiType = PoiType(poiType.asInstanceOf[String]),
        name = name.asInstanceOf[String])
    }
  }
}
