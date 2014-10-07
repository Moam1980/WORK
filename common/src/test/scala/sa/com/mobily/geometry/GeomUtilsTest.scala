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

    val sridPolar = 4326
    val sridPlanar = 32638
    val geomFactory = JTSFactoryFinder.getGeometryFactory

    val polygonWkt = "POLYGON (( 0 0, 1 0, 1 1, 0 1, 0 0 ))"
    val poly = geomFactory.createPolygon(
      Array(new Coordinate(0, 0), new Coordinate(1, 0), new Coordinate(1, 1), new Coordinate(0, 1),
        new Coordinate(0, 0)))
    poly.setSRID(sridPolar)

    val position = geomFactory.createPoint(new Coordinate(0, 0))
    position.setSRID(sridPlanar)

    val expectedCircle = GeomUtils.parseWkt(
      "POLYGON ((1 0, 0.71 0.71, 0 1, -0.71 0.71, -1 0, -0.71 -0.71, 0 -1, 0.71 -0.71, 1 0))",
      sridPlanar)
    val expectedCirSect = GeomUtils.parseWkt(
      "POLYGON ((0 0, 0 -1, 0.38 -0.92, 0.71 -0.71, 0.92 -0.38, 1 0, 0 0))",
      sridPlanar)
    val expectedHippopede = GeomUtils.parseWkt(
      "POLYGON ((0 0, -176.66 -11.11, -247.38 -31.25, -297.99 -56.85, -336.14 -86.31, -364.57 -118.46, "
        + "-384.64 -152.29, -397.12 -186.87, -402.61 -221.34, -401.57 -254.85, -394.49 -286.61, -381.83 -315.88, "
        + "-364.12 -341.94, -341.94 -364.12, -315.88 -381.83, -286.61 -394.49, -254.85 -401.57, -221.34 -402.61, "
        + "-186.87 -397.12, -152.29 -384.64, -118.46 -364.57, -86.31 -336.14, -56.85 -297.99, -31.25 -247.38, "
        + "-11.11 -176.66, 0 0))",
      sridPlanar)
    val expectedConchoid = GeomUtils.parseWkt(
      "POLYGON ((0 0, 46.01 28.2, 83.65 71.45, 108.3 126.8, 116.41 189.97, 105.91 255.7, 76.41 318.28, 29.28 372.08, "
        + "-32.43 412.08, -104.28 434.37, -180.81 436.51, -256.03 417.8, -324 379.36, -379.36 324, -417.8 256.03, "
        + "-436.51 180.81, -434.37 104.28, -412.08 32.43, -372.08 -29.28, -318.28 -76.41, -255.7 -105.91, "
        + "-189.97 -116.41, -126.8 -108.3, -71.45 -83.65, -28.2 -46.01, 0 0))",
      sridPlanar)
    val expectedHippopedeWithBackLobe = GeomUtils.parseWkt(
      "POLYGON ((-3.14 -49.9, -11.11 -176.66, -31.25 -247.38, -56.85 -297.99, -86.31 -336.14, -118.46 -364.57, "
        + "-152.29 -384.64, -186.87 -397.12, -221.34 -402.61, -254.85 -401.57, -286.61 -394.49, -315.88 -381.83, "
        + "-341.94 -364.12, -364.12 -341.94, -381.83 -315.88, -394.49 -286.61, -401.57 -254.85, -402.61 -221.34, "
        + "-397.12 -186.87, -384.64 -152.29, -364.57 -118.46, -336.14 -86.31, -297.99 -56.85, -247.38 -31.25, "
        + "-176.66 -11.11, -49.8 -3.13, -50 0, -49.61 6.27, -48.43 12.43, -46.49 18.41, -43.82 24.09, -40.45 29.39, "
        + "-36.45 34.23, -31.87 38.53, -26.79 42.22, -21.29 45.24, -15.45 47.55, -9.37 49.11, -3.14 49.9, 3.14 49.9, "
        + "9.37 49.11, 15.45 47.55, 21.29 45.24, 26.79 42.22, 31.87 38.53, 36.45 34.23, 40.45 29.39, 43.82 24.09, "
        + "46.49 18.41, 48.43 12.43, 49.61 6.27, 50 0, 49.61 -6.27, 48.43 -12.43, 46.49 -18.41, 43.82 -24.09, "
        + "40.45 -29.39, 36.45 -34.23, 31.87 -38.53, 26.79 -42.22, 21.29 -45.24, 15.45 -47.55, 9.37 -49.11, "
        + "3.14 -49.9, -3.14 -49.9))",
      sridPlanar)
    val expectedConchoidWithBackLobe = GeomUtils.parseWkt(
      "POLYGON ((-26.11 -42.59, -28.2 -46.01, -71.45 -83.65, -126.8 -108.3, -189.97 -116.41, -255.7 -105.91, "
        + "-318.28 -76.41, -372.08 -29.28, -412.08 32.43, -434.37 104.28, -436.51 180.81, -417.8 256.03, -379.36 324, "
        + "-324 379.36, -256.03 417.8, -180.81 436.51, -104.28 434.37, -32.43 412.08, 29.28 372.08, 76.41 318.28, "
        + "105.91 255.7, 116.41 189.97, 108.3 126.8, 83.65 71.45, 46.01 28.2, 42.55 26.08, 43.82 24.09, 46.49 18.41, "
        + "48.43 12.43, 49.61 6.27, 50 0, 49.61 -6.27, 48.43 -12.43, 46.49 -18.41, 43.82 -24.09, 40.45 -29.39, "
        + "36.45 -34.23, 31.87 -38.53, 26.79 -42.22, 21.29 -45.24, 15.45 -47.55, 9.37 -49.11, 3.14 -49.9, -3.14 -49.9, "
        + "-9.37 -49.11, -15.45 -47.55, -21.29 -45.24, -26.11 -42.59))",
      sridPlanar)
  }

  "GeomUtils" should "parse WKT text and assign SRID" in new WithShapes {
    GeomUtils.parseWkt(polygonWkt, sridPolar) should equalGeometry (poly)
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
