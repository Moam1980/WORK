/*
 * TODO: License goes here!
 */

package sa.com.mobily.location

import org.apache.spark.sql._
import org.scalatest.{FlatSpec, ShouldMatchers}

import sa.com.mobily.geometry.Coordinates
import sa.com.mobily.parsing.{CsvParser, RowParser}
import sa.com.mobily.poi.{Home, Poi}
import sa.com.mobily.user.User

class LocationPoiViewTest extends FlatSpec with ShouldMatchers {

  import LocationPoiView._

  trait WithLocationPoiView {

    val line = "420030100040377|420|locationTest|Home|1"
    val row1 = Row("420030100040377", "420", "locationTest", Row("Home"), 1D)
    val wrongRow = Row("420030100040377", "420", "locationTest", "Home")
    val fields: Array[String] = Array("420030100040377", "420", "locationTest", "Home", "1.0")

    val header: Array[String] = Array("imsi", "mcc", "name", "poi-type", "weight")
    val polygonWkt = "POLYGON (( 0 0, 1 0, 1 1, 0 1, 0 0 ))"
    val user = User(imei = "866173010386736", imsi = "420030100040377", msisdn = 560917079L)

    val locationPoiView = LocationPoiView(
      imsi = "420030100040377",
      mcc = "420",
      name = "locationTest",
      poiType = Home,
      weight = 1)
    val location = Location(
      name = "locationTest",
      client = "clientTest",
      epsg = Coordinates.Wgs84GeodeticEpsg,
      geomWkt = polygonWkt)
    val poi = Poi(user = user, poiType = Home, geomWkt = polygonWkt, countryIsoCode = "es")
  }

  "LocationPoiView" should "be built from CSV" in new WithLocationPoiView {
    CsvParser.fromLine(line).value.get should be (locationPoiView)
  }

  it should "be discarded when CSV format is wrong" in new WithLocationPoiView {
    an [Exception] should be thrownBy fromCsv.fromFields(fields.updated(15, ""))
  }

  it should "be built from Row" in new WithLocationPoiView {
    RowParser.fromRow(row1) should be (locationPoiView)
  }

  it should "be discarded when Row format is wrong" in new WithLocationPoiView {
    an [Exception] should be thrownBy fromRow.fromRow(wrongRow)
  }

  it should "return correct fields" in new WithLocationPoiView {
    locationPoiView.fields should be (fields)
  }

  it should "return correct header" in new WithLocationPoiView {
    LocationPoiView.Header should be (header)
  }

  it should "field and header have same size" in new WithLocationPoiView {
    locationPoiView.fields.size should be (LocationPoiView.Header.size)
  }

  it should "apply LocationPoiView built from Location and Poi" in new WithLocationPoiView {
    LocationPoiView(location, poi) should be (locationPoiView)
  }
}
