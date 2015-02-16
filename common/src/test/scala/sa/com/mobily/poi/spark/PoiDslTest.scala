/*
 * TODO: License goes here!
 */

package sa.com.mobily.poi.spark

import scala.reflect.io.File

import org.apache.spark.sql.Row
import org.scalatest.{FlatSpec, ShouldMatchers}

import sa.com.mobily.geometry.GeomUtils
import sa.com.mobily.poi.{Work, Poi}
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
}
