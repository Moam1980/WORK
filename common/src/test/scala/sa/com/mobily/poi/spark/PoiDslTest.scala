/*
 * TODO: License goes here!
 */

package sa.com.mobily.poi.spark

import scala.reflect.io.File

import org.apache.spark.sql.Row
import org.scalatest.{FlatSpec, ShouldMatchers}

import sa.com.mobily.geometry.{Coordinates, GeomUtils}
import sa.com.mobily.poi._
import sa.com.mobily.user.User
import sa.com.mobily.utils.LocalSparkSqlContext

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
    val geomToAnalyze = GeomUtils.parseWkt("POLYGON ((1 5, 5 5, 5 1, 1 1, 1 5))", Coordinates.SaudiArabiaUtmSrid)
    val homeGeom = "POLYGON ((2 2, 2 4, 4 4, 4 2, 2 2))"
    val workGeom = "POLYGON ((0 2, 2 2, 2 0, 0 0, 0 2))"
    val pois = sc.parallelize(Seq(Poi(user1, Home, homeGeom), Poi(user1, Work, workGeom), Poi(user2, work, workGeom)))
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
  }

  "EventDsl" should "get correctly parsed rows" in new WithPoisRows {
    rows.toPoi.count should be (1)
  }

  it should "save events in parquet" in new WithPoisRows {
    val path = File.makeTemp().name
    pois.saveAsParquetFile(path)
    sqc.parquetFile(path).toPoi.collect.sameElements(pois.collect) should be (true)
    File(path).deleteRecursively
  }

  it should "generate the poi metrics for a location" in new WithPoisInGeometry {
    private val collect: Array[Poi] = pois.collect
    pois.locationPoiMetrics(geomToAnalyze) should be (LocationPoiMetrics(
      0.5,
      0.3535533905932738,
      1.0,
      0.25,
      2,
      3,
      Map(List(Home, Work) -> 1, List(Work) -> 1)))
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
