/*
 * TODO: License goes here!
 */

package sa.com.mobily.poi

import org.scalatest._

import sa.com.mobily.cell.{EgBts, TwoG}
import sa.com.mobily.geometry.UtmCoordinates
import sa.com.mobily.utils.LocalSparkContext

class UserActivityPoiTest extends FlatSpec with ShouldMatchers with LocalSparkContext {

  trait WithGeometry {

    val coords1 = UtmCoordinates(821375.9, 3086866.0)
    val coords2 = UtmCoordinates(821485.9, 3086976.0)
    val egBts1 = EgBts("6539", "6539", "New-Addition", coords1, "", "", 57, "BTS", "Alcatel", "E317",
      "42003000576539", TwoG, "17", "Macro", 0, 0)
    val egBts2 = EgBts("6540", "6540", "New-Addition", coords2, "", "", 57, "BTS", "Alcatel", "E317",
      "42003000576539", TwoG, "17", "Macro", 535.49793639, 681.54282813)
    val geometry1 = egBts1.geom
    val geometry2 = egBts2.geom
  }

  trait WithUserActivity extends WithGeometry {

    val firstUserSiteId = "2541"
    val secondUserSiteId = "2566"
    val firstUserRegionId = 10.toShort
    val secondUserRegionId = 20.toShort
    val poiLocation1 = (firstUserSiteId, firstUserRegionId)
    val poiLocation2 = (secondUserSiteId, secondUserRegionId)
    val poisLocation = Seq(poiLocation1, poiLocation2)
    val btsCatalogue =
      Map(
        (firstUserSiteId, firstUserRegionId) -> Seq(egBts1),
        (secondUserSiteId, secondUserRegionId) -> Seq(egBts2),
        ("9999", 30.toShort) -> Seq())
  }

  "UserActivityPoi" should "find geometries" in new WithUserActivity {
    val geometries = UserActivityPoi.findGeometries(poisLocation, btsCatalogue)

    geometries should be(Seq(geometry1, geometry2))
  }
}
