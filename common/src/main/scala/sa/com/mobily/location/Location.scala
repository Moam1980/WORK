/*
 * TODO: License goes here!
 */

package sa.com.mobily.location

import com.vividsolutions.jts.geom.{Geometry, PrecisionModel}

import sa.com.mobily.geometry._
import sa.com.mobily.parsing.{OpenCsvParser, CsvParser}

/** Client information **/
case class Client(
    id: Int,
    name: String) {

  def fields: Array[String] = Array(id.toString, name)

  override def equals(other: Any): Boolean = other match {
    case that: Client => (that canEqual this) && (this.id == that.id)
    case _ => false
  }

  override def hashCode: Int = id.hashCode

  def canEqual(other: Any): Boolean = other.isInstanceOf[Client]
}

object Client {

  def header: Array[String] = Array("id", "name")
}

/** Location information */
case class Location(
    id: Int,
    name: String,
    client: Client,
    epsg: String,
    precision: Double,
    geomWkt: String) {

  lazy val geom: Geometry = GeomUtils.parseWkt(geomWkt, srid, precisionModel)

  def fields: Array[String] = Array(id.toString, name) ++ client.fields ++ Array(epsg, precision.toString, geomWkt)

  override def equals(other: Any): Boolean = other match {
    case that: Location => (that canEqual this) && (this.id == that.id) && (this.client == that.client)
    case _ => false
  }

  override def hashCode: Int = id.hashCode

  def canEqual(other: Any): Boolean = other.isInstanceOf[Location]

  def srid: Int = Coordinates.srid(epsg)

  def precisionModel: PrecisionModel = new PrecisionModel(precision)
}

object Location {

  def header: Array[String] = Array("id", "name") ++ Client.header ++ Array("epsg", "precision", "geomWkt")

  final val lineCsvParserObject = new OpenCsvParser(quote = '"')

  implicit val fromCsv = new CsvParser[Location] {

    override def lineCsvParser: OpenCsvParser = lineCsvParserObject

    override def fromFields(fields: Array[String]): Location = {
      val Array(idText, nameText, clientIdText, clientNameText, epsgText, precisionText, geomWktText) = fields

      Location(
        id = idText.toInt,
        name = nameText,
        client = Client(id = clientIdText.toInt, name = clientNameText),
        epsg = epsgText,
        precision = precisionText.toDouble,
        geomWkt = geomWktText)
    }
  }
}
