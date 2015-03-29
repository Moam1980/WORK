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

class LocationPointViewTest extends FlatSpec with ShouldMatchers {

  import LocationPointView._

  trait WithLocationPointView {

    val line = "clientTest|locationTest|1|1|26.46564199|50.07687119"
    val row1 = Row("clientTest", "locationTest", 1, 1, 26.46564199D, 50.07687119D)
    val wrongRow = Row("clientTest", "locationTest", 1, "NaN", 26.46564199D, 50.07687119D)
    val wrongRow2 = Row("clientTest", "locationTest", "NaN", 1, 26.46564199D, 50.07687119D)

    val fields: Array[String] = Array("clientTest", "locationTest", "1", "1", "26.46564199", "50.07687119")

    val header: Array[String] = Array("client", "name", "geometryOrder", "pointOrder", "latitude", "longitude")

    val locationPointView = LocationPointView(
      client = "clientTest",
      name = "locationTest",
      geometryOrder = 1,
      pointOrder = 1,
      latitude = 26.46564199D,
      longitude = 50.07687119D)

    val location = Location(
      name = "locationTest",
      client = "clientTest",
      epsg = Coordinates.SaudiArabiaUtmEpsg,
      geomWkt = "POINT (1006463.8 2937269.6)"
    )
  }

  "LocationPointView" should "be built from CSV" in new WithLocationPointView {
    CsvParser.fromLine(line).value.get should be (locationPointView)
  }

  it should "be discarded when CSV format is wrong" in new WithLocationPointView {
    an [Exception] should be thrownBy fromCsv.fromFields(fields.updated(4, ""))
  }

  it should "be built from Row" in new WithLocationPointView {
    RowParser.fromRow(row1) should be (locationPointView)
  }

  it should "be discarded when Row format is wrong" in new WithLocationPointView {
    an [Exception] should be thrownBy fromRow.fromRow(wrongRow)
  }

  it should "return correct fields" in new WithLocationPointView {
    locationPointView.fields should be (fields)
  }

  it should "return correct header" in new WithLocationPointView {
    LocationPointView.Header should be (header)
  }

  it should "field and header have same size" in new WithLocationPointView {
    locationPointView.fields.size should be (LocationPointView.Header.size)
  }

  it should "apply LocationPointView built from Location" in new WithLocationPointView {
    LocationPointView.normalizedWgs84Geom(location) should be (Array(locationPointView))
  }
}
