/*
 * TODO: License goes here!
 */

package sa.com.mobily.location

import com.vividsolutions.jts.geom.{Geometry, GeometryCollection, Point}
import com.vividsolutions.jts.geom.util.PolygonExtracter

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

  val Header: Array[String] = Array("name", "client", "epsg", "geomWkt")

  final val lineCsvParserObject = new OpenCsvParser

  implicit val fromCsv = new CsvParser[Location] {

    override def lineCsvParser: OpenCsvParser = lineCsvParserObject

    override def fromFields(fields: Array[String]): Location = {
      val Array(nameText, clientText, epsgText, geomWktText) = fields
      Location(name = nameText, client = clientText, epsg = epsgText, geomWkt = geomWktText)
    }
  }

  def isMatch(geom: Geometry, location: Location): Boolean = GeomUtils.safeIntersects(geom, location.geom)

  def bestMatch(geom: Geometry, locations: Seq[Location]): Location = geom match {
    case geom: GeometryCollection =>
      val polygons = PolygonExtracter.getPolygons(geom).toArray.map(_.asInstanceOf[Geometry])
      if (locations.exists(_.geom.isInstanceOf[Point]))
        locations.minBy(l =>
          polygons.minBy(_.getCentroid.distance(l.geom.getCentroid)).getCentroid.distance(l.geom.getCentroid))
      else
        locations.maxBy(l =>
          GeomUtils.intersectionRatio(polygons.maxBy(p => GeomUtils.intersectionRatio(p, l.geom)), l.geom))
    case _ =>
      if (locations.exists(_.geom.isInstanceOf[Point])) locations.minBy(_.geom.getCentroid.distance(geom.getCentroid))
      else locations.maxBy(l => GeomUtils.intersectionRatio(l.geom, geom))
  }
}
