/*
 * TODO: License goes here!
 */

package sa.com.mobily.location

import com.github.nscala_time.time.Imports._
import org.scalatest.{FlatSpec, ShouldMatchers}

import sa.com.mobily.geometry.{Coordinates, GeomUtils}
import sa.com.mobily.utils.EdmCustomMatchers

class LocationTest extends FlatSpec with ShouldMatchers with EdmCustomMatchers {

  import Location._

  trait WithLocation {

    val shapeWgs84Wkt = "POLYGON ((46.82329508 24.8484987, 46.82328424 24.84850154, " +
      "46.82279442 24.84870162, 46.82232655 24.84898089, 46.82189541 24.84933645, 46.82151477 24.84976271, " +
      "46.8211984 24.85025319, 46.82095802 24.85079782, 46.82080338 24.85138655, 46.82074419 24.85200661, " +
      "46.82078519 24.85264441, 46.82092913 24.85328457, 46.82117779 24.85391262, 46.82152994 24.85451322, " +
      "46.82197946 24.85507291, 46.82252018 24.85557641, 46.82314203 24.8560112, 46.82383397 24.8563675, " +
      "46.82458199 24.85663372, 46.82537016 24.85680375, 46.82618356 24.85687236, 46.82700531 24.85683612, " +
      "46.82781662 24.85669529, 46.82860071 24.85645276, 46.82934177 24.85611144, 46.83002412 24.85567966, " +
      "46.83063203 24.85516662, 46.83115479 24.8545824, 46.83158071 24.85394069, 46.8319021 24.85325602, " +
      "46.83211422 24.85254199, 46.83221334 24.8518149, 46.83220069 24.85109099, 46.83207747 24.85038378, " +
      "46.83184987 24.84971035, 46.83152599 24.84908325, 46.83111393 24.84851501, 46.83062768 24.84801538, " +
      "46.83007928 24.84759506, 46.82948561 24.84725836, 46.82885961 24.84701052, 46.82821913 24.84685405, " +
      "46.82757897 24.84678695, 46.82695592 24.84680721, 46.82636376 24.84690925, 46.82581524 24.84708568, " +
      "46.82532309 24.84732824, 46.82489704 24.84762506, 46.82489021 24.84763146, 46.82478932 24.84756679, " +
      "46.8246797 24.84751305, 46.82456334 24.84747294, 46.82444223 24.84744553, 46.82431837 24.8474335, " +
      "46.82419373 24.84743503, 46.82407027 24.84745008, 46.82395 24.84748044, 46.82383487 24.84752337, " +
      "46.82372685 24.84757976, 46.82362693 24.84764779, 46.82353705 24.84772653, 46.8234592 24.84781506, " +
      "46.82339432 24.84791155, 46.82334241 24.84801511, 46.82330541 24.8481239, 46.82328429 24.8482352, " +
      "46.82327804 24.84834904, 46.82328762 24.84846267, 46.82329508 24.8484987))"
    val geomWsg84 = GeomUtils.parseWkt(shapeWgs84Wkt, Coordinates.Wgs84GeodeticSrid, Coordinates.LatLongPrecisionModel)

    val locationLine = "locationTest|clientTest|" + Coordinates.Wgs84GeodeticEpsg + "|" + shapeWgs84Wkt
    val location = Location(name = "locationTest", client = "clientTest", epsg = Coordinates.Wgs84GeodeticEpsg,
      geomWkt = shapeWgs84Wkt)

    val locationHeader = Array("name", "client", "epsg", "geomWkt")
    val locationFields = Array("locationTest", "clientTest", Coordinates.Wgs84GeodeticEpsg, shapeWgs84Wkt)
  }

  trait WithMatchingLocations {

    val locationNoPoint1 = Location(name = "location", client = "client", epsg = Coordinates.SaudiArabiaUtmEpsg,
      geomWkt = "POLYGON ((0 0, 100 0, 100 100, 0 100, 0 0))")
    val locationNoPoint2 = locationNoPoint1.copy(geomWkt = "POLYGON ((100 0, 135 0, 135 100, 100 100, 100 0))")
    val matchingGeom1 = GeomUtils.parseWkt("POLYGON ((3 3, 13 3, 13 13, 3 13, 3 3))", locationNoPoint1.geom.getSRID)
    val nonMatchingGeom1 =
      GeomUtils.parseWkt("POLYGON ((103 103, 113 103, 113 113, 103 113, 103 103))", locationNoPoint1.geom.getSRID)

    val multiPolygonGeom = GeomUtils.parseWkt(
      "MULTIPOLYGON (((103 0, 103 101, 113 101, 113 0, 103 0)), ((37 37, 37 39, 39 39, 39 37, 37 37)), " +
        "((97 0, 97 100, 103 100, 103 0, 97 0)))",
      locationNoPoint1.geom.getSRID)
    val singlePolygonGeom =
      GeomUtils.parseWkt("POLYGON ((97 0, 97 100, 104 100, 104 0, 97 0))", locationNoPoint1.geom.getSRID)
    val locations = List(locationNoPoint1, locationNoPoint2)

    val locationPoint1 = locationNoPoint1.copy(geomWkt = "POINT (50 51)")
    val locationPoint2 = locationNoPoint1.copy(geomWkt = "POINT (130 50)")
    val pointLocations = List(locationPoint1, locationPoint2)
  }

  trait WithIntervals {

    val interval1 = new Interval(0, 3600000)
    val interval2 = new Interval(3600000, 7200000)
    val interval3 = new Interval(7200000, 10800000)
    val intervals = List(interval1, interval2, interval3)
  }

  "Location" should "return correct header" in new WithLocation {
    Location.header should be (locationHeader)
  }

  it should "return correct fields" in new WithLocation {
    location.fields should be (locationFields)
  }

  it should "return correct geometry" in new WithLocation {
    location.geom should equalGeometry(geomWsg84)
  }

  it should "be built from CSV" in new WithLocation {
    fromCsv.fromFields(locationFields) should be (location)
  }

  it should "be discarded when the CSV format is wrong" in new WithLocation {
    an [Exception] should be thrownBy fromCsv.fromFields(locationFields :+ "|")
  }

  it should "detect matching geometries" in new WithMatchingLocations {
    Location.isMatch(matchingGeom1, locationNoPoint1) should be (true)
  }

  it should "detect non-matching geometries" in new WithMatchingLocations {
    Location.isMatch(nonMatchingGeom1, locationNoPoint1) should be (false)
  }

  it should "take best match when there are no point geometries in locations (and geom is multipolygon)" in
    new WithMatchingLocations {
      Location.bestMatch(multiPolygonGeom, locations) should be (locationNoPoint1)
    }

  it should "take best match when there are no point geometries in locations (and geom is polygon)" in
    new WithMatchingLocations {
      Location.bestMatch(singlePolygonGeom, locations) should be (locationNoPoint2)
    }

  it should "take best match when there are point geometries in locations (and geom is multipolygon)" in
    new WithMatchingLocations {
      Location.bestMatch(multiPolygonGeom, pointLocations) should be (locationPoint1)
    }

  it should "take best match when there are point geometries in locations (and geom is polygon)" in
    new WithMatchingLocations {
      Location.bestMatch(singlePolygonGeom, pointLocations) should be (locationPoint2)
    }

  it should "create intervals from start/end and interval period in minutes" in new WithIntervals {
    Location.intervals(new DateTime(0), new DateTime(10800000), 60) should be (intervals)
  }
}
