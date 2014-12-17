/*
 * TODO: License goes here!
 */

package sa.com.mobily.poi

import org.scalatest.{FlatSpec, ShouldMatchers}

import sa.com.mobily.geometry.GeomUtils
import sa.com.mobily.user.User
import sa.com.mobily.utils.EdmCustomMatchers

class PoiTest extends FlatSpec with ShouldMatchers with EdmCustomMatchers {

  trait WithUser {

    val user = User(imei = "866173010386736", imsi = "420034122616618", msisdn = 560917079L)
  }

  trait WithGeometry {

    val sridPlanar = 32638
    val polygonWkt = "POLYGON (( 0 0, 1 0, 1 1, 0 1, 0 0 ))"
    val expectedCirSect = GeomUtils.parseWkt(polygonWkt, sridPlanar)
  }

  trait WithPoi extends WithUser with WithGeometry {

    val isoCode = "es"
    val poi = Poi(user = user, poiType = Work, geomWkt = polygonWkt, countryIsoCode = isoCode)
    val poiFields = user.fields ++ Array("Work", polygonWkt, isoCode)
    val header = User.header ++ Array("poiType", "geomWkt", "countryIsoCode")
  }

  "Poi" should "generate fields" in new WithPoi {
    poi.fields should be (poiFields)
  }

  it should "generate header" in new WithPoi {
    Poi.header should be (header)
  }

  it should "return the same number of fields for header and fields method" in new WithPoi {
    Poi.header.length == poi.fields.length should be(true)
  }

  it should "parse a geometry properly" in new WithPoi {
    poi.geometry should equalGeometry(expectedCirSect)
  }
}
