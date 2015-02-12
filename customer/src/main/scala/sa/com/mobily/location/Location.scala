/*
 * TODO: License goes here!
 */

package sa.com.mobily.location

import com.vividsolutions.jts.geom.Geometry

import sa.com.mobily.geometry.{Coordinates, GeomUtils}
import sa.com.mobily.parsing.{CsvParser, OpenCsvParser}

case class Location(
    name: String,
    client: String,
    epsg: String,
    geomWkt: String) {

  lazy val geom: Geometry = GeomUtils.parseWkt(geomWkt, Coordinates.srid(epsg), Coordinates.precisionModel(epsg))

  def fields: Array[String] = Array(name, client, epsg, geomWkt)
}

object Location {

  def header: Array[String] = Array("name", "client", "epsg", "geomWkt")

  final val lineCsvParserObject = new OpenCsvParser

  implicit val fromCsv = new CsvParser[Location] {

    override def lineCsvParser: OpenCsvParser = lineCsvParserObject

    override def fromFields(fields: Array[String]): Location = {
      val Array(nameText, clientText, epsgText, geomWktText) = fields

      Location(
        name = nameText,
        client = clientText,
        epsg = epsgText,
        geomWkt = geomWktText)
    }
  }
}
