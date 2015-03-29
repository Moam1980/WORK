/*
 * TODO: License goes here!
 */

package sa.com.mobily.location

import org.apache.spark.sql.Row

import sa.com.mobily.geometry.{Coordinates, GeomUtils}
import sa.com.mobily.parsing.{CsvParser, OpenCsvParser, RowParser}

case class LocationPointView(
    client: String,
    name: String,
    geometryOrder: Int,
    pointOrder: Int,
    latitude: Double,
    longitude: Double) {

  def fields: Array[String] =
    Array(client, name, geometryOrder.toString, pointOrder.toString, latitude.toString, longitude.toString)
}

object LocationPointView {

  val Header: Array[String] = Array("client", "name", "geometryOrder", "pointOrder", "latitude", "longitude")

  def normalizedWgs84Geom(location: Location): Array[LocationPointView] = {
    val geomWgs84 =
      GeomUtils.transformGeom(
        geom = location.geom,
        destSrid = Coordinates.Wgs84GeodeticSrid,
        destPrecisionModel = Coordinates.LatLongPrecisionModel,
        longitudeFirst = false)

    GeomUtils.geomAsPoints(geomWgs84).flatMap(geometryPoint => {
      geometryPoint._2.map(point =>
        LocationPointView(
          client = location.client,
          name = location.name,
          geometryOrder = geometryPoint._1,
          pointOrder = point._1,
          latitude = point._2._1,
          longitude = point._2._2)).toSeq}).toArray
  }

  implicit val fromCsv = new CsvParser[LocationPointView] {

    override def lineCsvParser: OpenCsvParser = new OpenCsvParser

    override def fromFields(fields: Array[String]): LocationPointView = {
      val Array(clientText, nameText, geometryOrderText, pointOrderText, latitudeText, longitudeText) = fields

      LocationPointView(
        client = clientText,
        name = nameText,
        geometryOrder = geometryOrderText.toInt,
        pointOrder = pointOrderText.toInt,
        latitude = latitudeText.toDouble,
        longitude = longitudeText.toDouble)
    }
  }

  implicit val fromRow = new RowParser[LocationPointView] {

    override def fromRow(row: Row): LocationPointView = {
      val Row(client, name, geometryOrder, pointOrder, latitude, longitude) = row

      LocationPointView(
        client = client.asInstanceOf[String],
        name = name.asInstanceOf[String],
        geometryOrder = geometryOrder.asInstanceOf[Int],
        pointOrder = pointOrder.asInstanceOf[Int],
        latitude = latitude.asInstanceOf[Double],
        longitude = longitude.asInstanceOf[Double])
    }
  }
}
