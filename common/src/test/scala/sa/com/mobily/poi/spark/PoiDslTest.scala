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
}
