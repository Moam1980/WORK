/*
 * TODO: License goes here!
 */

package sa.com.mobily.poi.spark

import scala.collection.Map
import scala.reflect.io.File

import org.apache.spark.sql.Row
import org.scalatest.{FlatSpec, ShouldMatchers}

import sa.com.mobily.geometry.GeomUtils
import sa.com.mobily.poi._
import sa.com.mobily.user.User
import sa.com.mobily.utils.{LocalSparkSqlContext, Stats}

class PoiDslTest extends FlatSpec with ShouldMatchers with LocalSparkSqlContext {

  import PoiDsl._

  trait WithUser {

    val user = User(imei = "866173010386736", imsi = "420034122616618", msisdn = 560917079L)
  }

  trait WithGeometry {

    val sridPlanar = 32638
    val polygonWkt = "POLYGON (( 0 0, 1 0, 1 1, 0 1, 0 0 ))"
    val expectedCirSect = GeomUtils.parseWkt(polygonWkt, sridPlanar)
  }

  trait WithPois extends WithUser with WithGeometry {

    val isoCode = "es"
    val poi = Poi(user = user, poiType = Work, geomWkt = polygonWkt, countryIsoCode = isoCode)
    val pois = sc.parallelize(Array(poi))
  }

  trait WithUsers {

    val user1 = User("", "", 1L)
    val user2 = User("", "", 2L)
  }

  trait WithPoisInGeometry extends WithUsers {

    val home = Home
    val work = Work
    val homeGeom = "POLYGON ((2 2, 2 4, 4 4, 4 2, 2 2))"
    val workGeom = "POLYGON ((0 2, 2 2, 2 0, 0 0, 0 2))"
    val pois = sc.parallelize(Seq(Poi(user1, Home, homeGeom), Poi(user1, Work, workGeom), Poi(user2, work, workGeom)))
  }

  trait WithPoisInGeometryMultipolygon extends WithUsers {

    val home = Home
    val work = Work
    val homeGeom = "MULTIPOLYGON ((( 0 0, 1 0, 1 1, 0 1, 0 0 )), (( 10 10, 11 10, 11 11, 10 11, 10 10 )))"
    val workGeom = "POLYGON ((0 2, 2 2, 2 0, 0 0, 0 2))"

    val distanceSubpolygonsArray =
      Array((Home,
        Stats(1L, 12.727922061357855D, 12.727922061357855D, 12.727922061357855D, 0D, 0D, 12.727922061357855D,
          12.727922061357855D, 12.727922061357855D, 12.727922061357855D, 12.727922061357855D, 12.727922061357855D,
          12.727922061357855D, 12.727922061357855D)))
    val distancePoisSubPolygonsStats =
      Map[PoiType, Stats](
        (Home,
        Stats(1L, 12.727922061357855D, 12.727922061357855D, 12.727922061357855D, 0D, 0D, 12.727922061357855D,
          12.727922061357855D, 12.727922061357855D, 12.727922061357855D, 12.727922061357855D, 12.727922061357855D,
          12.727922061357855D, 12.727922061357855D)))

    val pois = sc.parallelize(Seq(Poi(user1, Home, homeGeom), Poi(user1, Work, workGeom), Poi(user2, work, workGeom)))
  }

  trait WithPoisMultipolygon extends WithUsers {

    val home = Home
    val work = Work
    val homeGeom = "MULTIPOLYGON ((( 0 0, 1 0, 1 1, 0 1, 0 0 )), (( 10 10, 11 10, 11 11, 10 11, 10 10 )))"
    val distanceHome = Math.sqrt(9 * 9 + 9 * 9)
    val workGeom1 = "MULTIPOLYGON (((0 2, 2 2, 2 0, 0 0, 0 2)), (( 3 3, 4 3, 4 4, 3 4, 3 3 )))"
    val distanceWork1 = Math.sqrt(1 * 1 + 1 * 1)
    val workGeom2 = "MULTIPOLYGON (((20 20, 30 20, 30 30, 20 30, 20 20)), (( 50 50, 51 50, 51 51, 50 51, 50 50 )))"
    val distanceWork2 = Math.sqrt(20 * 20 + 20 * 20)

    val distanceSubpolygonsArray =
      Array((Home, Stats(Array(distanceHome))),
        (Work, Stats(Array(distanceWork1))),
        (Work, Stats(Array(distanceWork2))))
    val distancePoisSubPolygonsStats =
      Map[PoiType, Stats](
        (Home, Stats(Array(distanceHome))),
        (Work, Stats(Array(distanceWork1, distanceWork2))))

    val distancePerTypeCombination = Map((Seq(Home, Work), Stats(Array(0D))))

    val pois = sc.parallelize(Seq(Poi(user1, Home, homeGeom), Poi(user1, Work, workGeom1), Poi(user2, work, workGeom2)))
  }

  trait WithPoisRows extends WithPois {

    val row = Row(Row("866173010386736", "420034122616618", 560917079L), Row("Work"), polygonWkt, isoCode)
    val rows = sc.parallelize(List(row))
  }

  trait WithPoisAndUsers {

    val isoCode = "es"
    val user1 = User(
      imei = "3581870526733101",
      imsi = "420030100040377",
      msisdn = 966544312356L)
    val poiHomeUser1 =
      Poi(user = user1,
        poiType = Home,
        geomWkt = "POLYGON (( 0 0, 1 0, 1 1, 0 1, 0 0 ))",
        countryIsoCode = isoCode)
    val poiWorkUser1 =
      Poi(user = user1,
        poiType = Work,
        geomWkt = "POLYGON (( 100 100, 110 100, 110 110, 100 110, 100 100 ))",
        countryIsoCode = isoCode)
    val distanceHomeWorkUser1 = Math.sqrt(99 * 99 + 99 * 99)

    val user2 = User(
      imei = "",
      imsi = "420030000000002",
      msisdn = 0L)
    val poiHomeUser2 =
      Poi(user = user2,
        poiType = Home,
        geomWkt = "POLYGON (( 5 5, 6 5, 6 6, 5 6, 5 5 ))",
        countryIsoCode = isoCode)

    val user3 = User(
      imei = "",
      imsi = "420030000000003",
      msisdn = 0L)
    val poiHomeUser3 =
      Poi(user = user3,
        poiType = Home,
        geomWkt = "POLYGON (( 20 21, 23 21, 23 23, 20 21 ))",
        countryIsoCode = isoCode)
    val poiWorkUser3 =
      Poi(user = user3,
        poiType = Work,
        geomWkt = "POLYGON (( 120 100, 130 100, 130 140, 120 100 ))",
        countryIsoCode = isoCode)
    val distanceHomeWorkUser3 = Math.sqrt((120 - 23) * (120 - 23) + (100 - 23) * (100 - 23))

    val user4 = User(
      imei = "",
      imsi = "420030000000004",
      msisdn = 0L)
    val poiHomeUser4 =
      Poi(user = user4,
        poiType = Home,
        geomWkt = "POLYGON (( 50 50, 60 50, 60 60, 50 60, 50 50 ))",
        countryIsoCode = isoCode)
    val user5 = User(
      imei = "",
      imsi = "420030000000005",
      msisdn = 0L)

    val users = sc.parallelize(List(user1, user2, user3, user4, user5))
    val pois = sc.parallelize(List(poiHomeUser1, poiWorkUser1, poiHomeUser2, poiHomeUser3, poiWorkUser3, poiHomeUser4))

    val distancePerTypeCombination = Map((Seq(Home, Work), Stats(Array(distanceHomeWorkUser1, distanceHomeWorkUser3))))
  }

  "PoiDsl" should "get correctly parsed rows" in new WithPoisRows {
    rows.toPoi.count should be (1)
  }

  it should "save events in parquet" in new WithPoisRows {
    val path = File.makeTemp().name
    pois.saveAsParquetFile(path)
    sqc.parquetFile(path).toPoi.collect.sameElements(pois.collect) should be (true)
    File(path).deleteRecursively
  }

  it should "generate the poi metrics" in new WithPoisInGeometry {
    private val collect: Array[Poi] = pois.collect
    pois.poiMetrics should be (
      PoiMetrics(
        numUsers = 2,
        numPois = 3,
        numUsersPerTypeCombination = Map(List(Home, Work) -> 1, List(Work) -> 1),
        distancePoisPerTypeCombination = Map(List(Home, Work) -> Stats(Array(0D))),
        distancePoisSubPolygonsStats = Map[PoiType, Stats]()))
  }

  it should "generate the poi metrics with geometries" in new WithPoisInGeometryMultipolygon {
    private val collect: Array[Poi] = pois.collect
    pois.poiMetrics should be (
      PoiMetrics(
        numUsers = 2,
        numPois = 3,
        numUsersPerTypeCombination = Map(List(Home, Work) -> 1, List(Work) -> 1),
        distancePoisPerTypeCombination = Map(List(Home, Work) -> Stats(Array(0D))),
        distancePoisSubPolygonsStats = distancePoisSubPolygonsStats))
  }

  it should "generate distanceSubpolygonsPerType with POIs without multiple geometries" in new WithPois {
    pois.distanceSubpolygons.count should be (0)
  }

  it should "generate distanceSubpolygonsPerType with POIs (Home) with multiple geometries" in
    new WithPoisInGeometryMultipolygon {
      pois.distanceSubpolygons.collect should contain theSameElementsAs (distanceSubpolygonsArray)
    }

  it should "generate distanceSubpolygonsPerType with POIs (Home and Work) with multiple geometries" in
    new WithPoisMultipolygon {
      pois.distanceSubpolygons.collect should contain theSameElementsAs (distanceSubpolygonsArray)
    }

  it should "generate distancePerTypeCombination with users with one POI" in new WithPois {
    pois.distancePerTypeCombination.isEmpty should be (true)
  }

  it should "generate distancePerTypeCombination with one user with Home and Work" in new WithPoisMultipolygon {
    pois.distancePerTypeCombination should contain theSameElementsAs (distancePerTypeCombination)
  }

  it should "generate distancePerTypeCombination" in new WithPoisAndUsers {
    pois.distancePerTypeCombination should contain theSameElementsAs (distancePerTypeCombination)
  }

  it should "filter by users" in new WithPoisAndUsers {
    pois.filterByUsers(users).collect should contain theSameElementsAs(pois.collect)
  }

  it should "filter by user 1" in new WithPoisAndUsers {
    val users1 = sc.parallelize(List(user1))
    pois.filterByUsers(users1).collect should contain theSameElementsAs(Array(poiHomeUser1, poiWorkUser1))
  }

  it should "filter by users 1 and 2" in new WithPoisAndUsers {
    val users1And2 = sc.parallelize(List(user1, user2))
    val poisFiltered = Array(poiHomeUser1, poiWorkUser1, poiHomeUser2)
    pois.filterByUsers(users1And2).collect should contain theSameElementsAs(poisFiltered)
  }

  it should "filter by users 1 and 5" in new WithPoisAndUsers {
    val users1And2 = sc.parallelize(List(user1, user5))
    val poisFiltered = Array(poiHomeUser1, poiWorkUser1)
    pois.filterByUsers(users1And2).collect should contain theSameElementsAs(poisFiltered)
  }

  it should "filter by user 4" in new WithPoisAndUsers {
    val users1And2 = sc.parallelize(List(user4))
    val poisFiltered = Array(poiHomeUser4)
    pois.filterByUsers(users1And2).collect should contain theSameElementsAs(poisFiltered)
  }
}
