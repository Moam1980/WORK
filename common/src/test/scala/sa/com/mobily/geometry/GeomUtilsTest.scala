/*
 * TODO: License goes here!
 */

package sa.com.mobily.geometry

import com.vividsolutions.jts.geom.Coordinate
import org.geotools.geometry.jts.JTSFactoryFinder
import org.scalatest.{ShouldMatchers, FlatSpec}

import sa.com.mobily.utils.EdmCustomMatchers

class GeomUtilsTest extends FlatSpec with ShouldMatchers with EdmCustomMatchers {

  trait WithShapes {

    val sridPlanar = 32638
    val geomFactory = GeomUtils.geomFactory(sridPlanar)

    val polygonWkt = "POLYGON (( 0 0, 1 0, 1 1, 0 1, 0 0 ))"
    val poly = geomFactory.createPolygon(
      Array(new Coordinate(0, 0), new Coordinate(1, 0), new Coordinate(1, 1), new Coordinate(0, 1),
        new Coordinate(0, 0)))

    val position = geomFactory.createPoint(new Coordinate(0, 0))

    val expectedCircle = GeomUtils.parseWkt(
      "POLYGON ((1 0, 0.7 0.7, 0 1, -0.7 0.7, -1 0, -0.7 -0.7, 0 -1, 0.7 -0.7, 1 0))",
      sridPlanar)
    val expectedCirSect = GeomUtils.parseWkt(
      "POLYGON ((0 0, 0 -1, 0.4 -0.9, 0.7 -0.7, 0.9 -0.4, 1 0, 0 0))",
      sridPlanar)
    val expectedHippopede = GeomUtils.parseWkt(
      "POLYGON ((0 0, -176.7 -11.1, -247.4 -31.3, -298 -56.8, -336.1 -86.3, -364.6 -118.5, -384.6 -152.3, " +
        "-397.1 -186.9, -402.6 -221.3, -401.6 -254.8, -394.5 -286.6, -381.8 -315.9, -364.1 -341.9, -341.9 -364.1, " +
        "-315.9 -381.8, -286.6 -394.5, -254.8 -401.6, -221.3 -402.6, -186.9 -397.1, -152.3 -384.6, -118.5 -364.6, " +
        "-86.3 -336.1, -56.8 -298, -31.3 -247.4, -11.1 -176.7, 0 0))",
      sridPlanar)
    val expectedConchoid = GeomUtils.parseWkt(
      "POLYGON ((0 0, 46 28.2, 83.7 71.4, 108.3 126.8, 116.4 190, 105.9 255.7, 76.4 318.3, 29.3 372.1, -32.4 412.1, " +
        "-104.3 434.4, -180.8 436.5, -256 417.8, -324 379.4, -379.4 324, -417.8 256, -436.5 180.8, -434.4 104.3, " +
        "-412.1 32.4, -372.1 -29.3, -318.3 -76.4, -255.7 -105.9, -190 -116.4, -126.8 -108.3, -71.4 -83.7, -28.2 -46, " +
        "0 0))",
      sridPlanar)
    val expectedHippopedeWithBackLobe = GeomUtils.parseWkt(
      "POLYGON ((-3.1 -49.9, -11.1 -176.7, -31.3 -247.4, -56.8 -298, -86.3 -336.1, -118.5 -364.6, -152.3 -384.6, " +
        "-186.9 -397.1, -221.3 -402.6, -254.8 -401.6, -286.6 -394.5, -315.9 -381.8, -341.9 -364.1, -364.1 -341.9, " +
        "-381.8 -315.9, -394.5 -286.6, -401.6 -254.8, -402.6 -221.3, -397.1 -186.9, -384.6 -152.3, -364.6 -118.5, " +
        "-336.1 -86.3, -298 -56.8, -247.4 -31.3, -176.7 -11.1, -49.8 -3.1, -50 0, -49.6 6.3, -48.4 12.4, -46.5 18.4, " +
        "-43.8 24.1, -40.5 29.4, -36.4 34.2, -31.9 38.5, -26.8 42.2, -21.3 45.2, -15.5 47.6, -9.4 49.1, -3.1 49.9, " +
        "3.1 49.9, 9.4 49.1, 15.5 47.6, 21.3 45.2, 26.8 42.2, 31.9 38.5, 36.4 34.2, 40.5 29.4, 43.8 24.1, 46.5 18.4, " +
        "48.4 12.4, 49.6 6.3, 50 0, 49.6 -6.3, 48.4 -12.4, 46.5 -18.4, 43.8 -24.1, 40.5 -29.4, 36.4 -34.2, " +
        "31.9 -38.5, 26.8 -42.2, 21.3 -45.2, 15.5 -47.6, 9.4 -49.1, 3.1 -49.9, -3.1 -49.9))",
      sridPlanar)
    val expectedConchoidWithBackLobe = GeomUtils.parseWkt(
      "POLYGON ((-26.1 -42.6, -28.2 -46, -71.4 -83.7, -126.8 -108.3, -190 -116.4, -255.7 -105.9, -318.3 -76.4, " +
        "-372.1 -29.3, -412.1 32.4, -434.4 104.3, -436.5 180.8, -417.8 256, -379.4 324, -324 379.4, -256 417.8, " +
        "-180.8 436.5, -104.3 434.4, -32.4 412.1, 29.3 372.1, 76.4 318.3, 105.9 255.7, 116.4 190, 108.3 126.8, " +
        "83.7 71.4, 46 28.2, 42.6 26.1, 43.8 24.1, 46.5 18.4, 48.4 12.4, 49.6 6.3, 50 0, 49.6 -6.3, 48.4 -12.4, " +
        "46.5 -18.4, 43.8 -24.1, 40.5 -29.4, 36.4 -34.2, 31.9 -38.5, 26.8 -42.2, 21.3 -45.2, 15.5 -47.6, 9.4 -49.1, " +
        "3.1 -49.9, -3.1 -49.9, -9.4 -49.1, -15.5 -47.6, -21.3 -45.2, -26.1 -42.6))",
      sridPlanar)
  }

  "GeomUtils" should "parse WKT text and assign SRID" in new WithShapes {
    GeomUtils.parseWkt(polygonWkt, sridPlanar) should equalGeometry (poly)
  }

  it should "write WKT text (without SRID)" in new WithShapes {
    GeomUtils.wkt(expectedCirSect) should be ("POLYGON ((0 0, 0 -1, 0.4 -0.9, 0.7 -0.7, 0.9 -0.4, 1 0, 0 0))")
  }

  it should "build a circle" in new WithShapes {
    GeomUtils.circle(position, 1, 8) should equalGeometry (expectedCircle)
  }
  
  it should "build a circular sector polygon" in new WithShapes {
    GeomUtils.circularSector(position, 135, 90, 1, 5) should equalGeometry (expectedCirSect)
  }

  it should "build a hippopede polygon" in new WithShapes {
    GeomUtils.hippopede(position, 500, 225, 90, 25) should equalGeometry (expectedHippopede)
  }

  it should "build a conchoid polygon" in new WithShapes {
    GeomUtils.conchoid(position, 500, 315, 225, 25) should equalGeometry (expectedConchoid)
  }

  it should "add the proper back lobe in a hippopede" in new WithShapes {
    GeomUtils.addBackLobe(expectedHippopede, position, 500, 0.1) should equalGeometry (expectedHippopedeWithBackLobe)
  }

  it should "add the proper back lobe in a conchoid" in new WithShapes {
    GeomUtils.addBackLobe(expectedConchoid, position, 500, 0.1) should equalGeometry (expectedConchoidWithBackLobe)
  }

  it should "convert to an angle from an azimuth" in new WithShapes {
    GeomUtils.azimuthToAngle(0) should be (90)
    GeomUtils.azimuthToAngle(90) should be (0)
    GeomUtils.azimuthToAngle(180) should be (-90)
    GeomUtils.azimuthToAngle(270) should be (-180)
  }
}
