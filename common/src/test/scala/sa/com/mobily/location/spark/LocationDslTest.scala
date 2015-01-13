/*
 * TODO: License goes here!
 */

package sa.com.mobily.location.spark

import org.scalatest._

import sa.com.mobily.cell.{Cell, FourGFdd, Micro}
import sa.com.mobily.cell.spark.CellDsl._
import sa.com.mobily.geometry.UtmCoordinates
import sa.com.mobily.location.{Client, Location}
import sa.com.mobily.utils.LocalSparkContext

class LocationDslTest extends FlatSpec with ShouldMatchers with LocalSparkContext {

  import LocationDsl._

  trait WithLocationText {

    val location1 = "\"0\"|\"locationTest\"|\"0\"|\"clientTest\"|\"EPSG:4326\"|\"1e7\"|" +
      "\"POLYGON ((0 0, 0 2, 2 2, 2 0, 0 0))\""
    val location2 = "\"1\"|\"other\"|\"2\"|\"clientOther\"|\"EPSG:32638\"|\"10\"|" +
      "\"POLYGON ((0 0, 0 2, 2 2, 2 0, 0 0))\""
    val location3 = "\"WrongNumber\"|\"other\"|\"2\"|\"clientOther\"|\"EPSG:32638\"|\"10\"|" +
      "\"POLYGON ((0 0, 0 2, 2 2, 2 0, 0 0))\""

    val locations = sc.parallelize(List(location1, location2, location3))
  }

  trait LocationsWithCellCatalogueEmpty {

    val client = Client(id = 0, name = "clientTest")
    val location1 = Location(id = 0, name = "locationTest", client = client, epsg = "EPSG:32638", precision = 10d,
      geomWkt = "POLYGON ((0 0, 0 2, 2 2, 2 0, 0 0))")
    val location2 = Location(id = 1, name = "locationTest1", client = client, epsg = "EPSG:32638", precision = 10d,
      geomWkt = "POLYGON ((2.5 2.5, 2.5 3.5, 3.5 3.5, 3.5 2.5, 2.5 2.5))")
    val location3 = Location(id = 2, name = "locationTest2", client = client, epsg = "EPSG:32638", precision = 10d,
      geomWkt = "POLYGON ((4.5 4.5, 4.5 5.5, 5.5 5.5, 5.5 4.5, 4.5 4.5))")

    val location1WithCells1 = (location1, List())
    val location1WithCells2 = (location2, List())
    val location1WithCells3 = (location3, List())

    val locations = sc.parallelize(Array(location1, location2, location3))

    implicit val cellCatalogue = sc.parallelize(Array[Cell]()).toBroadcastMap
  }

  trait CellCatalogue {

    val cell1 = Cell(1, 1, UtmCoordinates(1, 4), FourGFdd, Micro, 20, 180, 45, 4, "1",
      "POLYGON ((0 0, 0 2, 2 2, 2 0, 0 0))")
    val cell2 = cell1.copy(cellId = 2, coverageWkt = "POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))")
    val cell3 = cell1.copy(cellId = 3, coverageWkt = "POLYGON ((0.5 0, 0.5 1, 1.5 1, 1.5 0, 0.5 0))")
    val cell4 = cell1.copy(cellId = 4, coverageWkt = "POLYGON ((4 4, 4 5, 5 5, 5 4, 4 4))")

    implicit val cellCatalogue = sc.parallelize(Array(cell1, cell2, cell3, cell4)).toBroadcastMap
  }

  trait LocationsWithCellCatalogue extends CellCatalogue {

    val client = Client(id = 0, name = "clientTest")
    val location1 = Location(id = 0, name = "locationTest", client = client, epsg = "EPSG:32638", precision = 10d,
        geomWkt = "POLYGON ((0 0, 0 2, 2 2, 2 0, 0 0))")
    val location2 = Location(id = 1, name = "locationTest1", client = client, epsg = "EPSG:32638", precision = 10d,
      geomWkt = "POLYGON ((2.5 2.5, 2.5 3.5, 3.5 3.5, 3.5 2.5, 2.5 2.5))")
    val location3 = Location(id = 2, name = "locationTest2", client = client, epsg = "EPSG:32638", precision = 10d,
      geomWkt = "POLYGON ((4.5 4.5, 4.5 5.5, 5.5 5.5, 5.5 4.5, 4.5 4.5))")

    val location1WithCells1 = (location1, List(cell1, cell2, cell3))
    val location1WithCells2 = (location2, List())
    val location1WithCells3 = (location3, List(cell4))

    val locations = sc.parallelize(Array(location1, location2, location3))
  }

  trait LocationsAnotherPrecisionWithCellCatalogue extends CellCatalogue {

    val client = Client(id = 0, name = "clientTest")
    val location1 = Location(id = 0, name = "locationTest", client = client, epsg = "EPSG:32638", precision = 1e7d,
      geomWkt = "POLYGON ((0 0, 0 2, 2 2, 2 0, 0 0))")
    val location2 = Location(id = 1, name = "locationTest1", client = client, epsg = "EPSG:32638", precision = 1e7d,
      geomWkt = "POLYGON ((2.5 2.5, 2.5 3.5, 3.5 3.5, 3.5 2.5, 2.5 2.5))")
    val location3 = Location(id = 2, name = "locationTest2", client = client, epsg = "EPSG:32638", precision = 1e7d,
      geomWkt = "POLYGON ((4.5 4.5, 4.5 5.5, 5.5 5.5, 5.5 4.5, 4.5 4.5))")

    val location1WithCells1 = (location1, List(cell1, cell2, cell3))
    val location1WithCells2 = (location2, List())
    val location1WithCells3 = (location3, List(cell4))

    val locations = sc.parallelize(Array(location1, location2, location3))
  }

  trait LocationWsg84WithCellCatalogue extends CellCatalogue {

    val client = Client(id = 0, name = "clientTest")
    val location1Wsg84 = Location(id = 0, name = "locationTest", client = client, epsg = "EPSG:4326", precision = 1e12d,
      geomWkt = "POLYGON ((0.000000000000 40.511256115293, 0.000018038752 40.511256115293, " +
        "0.000018038752 40.511274033286, 0.000000000000 40.511274033287, 0.000000000000 40.511256115293))")

    val location1WithCells1 = (location1Wsg84, List(cell1, cell2, cell3))

    val locations = sc.parallelize(Array(location1Wsg84))
  }
  
  "LocationDsl" should "get correctly parsed data" in new WithLocationText {
    locations.toLocation.count should be (2)
  }

  it should "get errors when parsing data" in new WithLocationText {
    locations.toLocationErrors.count should be (1)
  }

  it should "get both correctly and wrongly parsed data" in new WithLocationText {
    locations.toParsedLocation.count should be (3)
  }
  
  it should "get cells for location" in new LocationsWithCellCatalogue {
    val locationsWithCells = locations.intersectingCells(cellCatalogue, false)
    locationsWithCells.count should be (3)
    locationsWithCells.collect should be (Array(location1WithCells1, location1WithCells2, location1WithCells3))
  }

  it should "get cells for location with another precision" in new LocationsAnotherPrecisionWithCellCatalogue {
    val locationsWithCells = locations.intersectingCells(cellCatalogue, false)
    locationsWithCells.count should be (3)
    locationsWithCells.collect should be (Array(location1WithCells1, location1WithCells2, location1WithCells3))
  }

  it should "get cells for location when location is WSG84" in new LocationWsg84WithCellCatalogue {
    val locationsWithCells = locations.intersectingCells(cellCatalogue, false)
    locationsWithCells.count should be (1)
    locationsWithCells.collect should be (Array(location1WithCells1))
  }

  it should "get cells for location with longitude first true" in new LocationsWithCellCatalogue {
    val locationsWithCells = locations.intersectingCells(cellCatalogue, true)
    locationsWithCells.count should be (3)
    locationsWithCells.collect should be (Array(location1WithCells1, location1WithCells2, location1WithCells3))
  }

  it should "get cells for location when no catalogue" in new LocationsWithCellCatalogueEmpty {
    val locationsWithCells = locations.intersectingCells(cellCatalogue, false)
    locationsWithCells.count should be (3)
    locationsWithCells.collect should be (Array(location1WithCells1, location1WithCells2, location1WithCells3))
  }
}
