/*
 * TODO: License goes here!
 */

package sa.com.mobily.usercentric

import org.scalatest.{ShouldMatchers, FlatSpec}

import sa.com.mobily.geometry.{Coordinates, GeomUtils}
import sa.com.mobily.utils.EdmCustomMatchers

class DwellTest extends FlatSpec with ShouldMatchers with EdmCustomMatchers {

  trait WithDwell {

    val dwell = Dwell(
      user = 1,
      startTime = 1,
      endTime = 10,
      geomWkt = "POLYGON ((3 3, 3 6, 6 6, 6 3, 3 3))",
      cells = Set((2, 4), (2, 6)),
      firstEventBeginTime = 3,
      lastEventEndTime = 9)
  }

  "Dwell" should "build geometry from WKT" in new WithDwell {
    dwell.geom should
      equalGeometry(GeomUtils.parseWkt("POLYGON ((3 3, 3 6, 6 6, 6 3, 3 3))", Coordinates.SaudiArabiaUtmSrid))
  }
}
