/*
 * TODO: License goes here!
 */

package sa.com.mobily.poi

import org.scalatest._

import sa.com.mobily.cell.{EgBts, TwoG}
import sa.com.mobily.geometry.{GeomUtils, UtmCoordinates}
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
    val firstUserRegionId = "1"
    val secondUserRegionId = "2"
    val poiLocation1 = (firstUserSiteId, firstUserRegionId)
    val poiLocation2 = (secondUserSiteId, secondUserRegionId)
    val poisLocation = Seq(poiLocation1, poiLocation2)
    val btsCatalogue =
      Map(
        (firstUserSiteId, firstUserRegionId) -> Seq(egBts1),
        (secondUserSiteId, secondUserRegionId) -> Seq(egBts2),
        ("9999", "3") -> Seq())
  }

  trait WithUnionGeometries extends WithGeometry {

    val shapeWkt1 = "POLYGON (( 130 10, 130 14, 2 14, 2 10, 130 10 ))"
    val shapeWkt2 = "POLYGON (( 13 14, 17 14, 17 10, 13 14 ))"
    val shapeWkt3 = "POLYGON (( 230 0, 230 4, 2 4, 2 0, 230 0 ))"
    val shapeWkt4 = "POLYGON (( 13 114, 17 114, 17 10, 13 114 ))"
    val shapeWktUnion = "MULTIPOLYGON (((230 0, 2 0, 2 4, 230 4, 230 0)), ((17 14, 130 14, 130 10, 2 10, 2 14, 16.8 14, " +
      "13 114, 17 114, 17 14)))"

    val geom1 = GeomUtils.parseWkt(shapeWkt1 , coords1.srid)
    val geom2 = GeomUtils.parseWkt(shapeWkt2 , coords1.srid)
    val geom3 = GeomUtils.parseWkt(shapeWkt3 , coords1.srid)
    val geom4 = GeomUtils.parseWkt(shapeWkt4 , coords1.srid)
    val geomUnion = GeomUtils.parseWkt(shapeWktUnion , coords1.srid)
    val geoms = Seq(geom1, geom2, geom3, geom4)
  }

  "UserActivityPoi" should "find geometries" in new WithUserActivity {
    val geometries = UserActivityPoi.findGeometries(poisLocation, btsCatalogue)
    geometries should be(Seq(geometry1, geometry2))
  }

  "it" should "union geometries and apply the Douglas Peucker Simplifier" in new WithUnionGeometries {
    UserActivityPoi.unionGeoms(geoms) should be (geomUnion)
  }
}
