/*
 * TODO: License goes here!
 */

package sa.com.mobily.geometry

import com.vividsolutions.jts.geom.{PrecisionModel, Coordinate}
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

    val utm38NPolygonWkt = "POLYGON ((684233.4 2749404.5, 684232.3 2749404.8, 684182.5 2749426.3, " +
      "684134.8 2749456.6, 684090.7 2749495.4, 684051.6 2749542.1, 684018.9 2749596, 683993.8 2749656, " +
      "683977.3 2749721, 683970.4 2749789.6, 683973.6 2749860.3, 683987.2 2749931.4, 684011.4 2750001.3, " +
      "684046.1 2750068.3, 684090.7 2750130.9, 684144.6 2750187.4, 684206.8 2750236.4, 684276.2 2750276.8, " +
      "684351.4 2750307.3, 684430.8 2750327.2, 684512.9 2750335.9, 684596 2750333, 684678.2 2750318.5, " +
      "684757.8 2750292.7, 684833.2 2750255.9, 684902.8 2750209, 684965 2750153, 685018.7 2750089, " +
      "685062.7 2750018.5, 685096.2 2749943.1, 685118.7 2749864.3, 685129.8 2749783.9, 685129.6 2749703.7, " +
      "685118.2 2749625.2, 685096.2 2749550.3, 685064.4 2749480.4, 685023.6 2749416.9, 684975.2 2749360.9, " +
      "684920.4 2749313.6, 684860.9 2749275.5, 684798 2749247.2, 684733.5 2749229, 684668.9 2749220.7, " +
      "684605.9 2749222.1, 684545.9 2749232.6, 684490.2 2749251.4, 684440.1 2749277.6, 684396.6 2749309.9, " +
      "684395.9 2749310.6, 684385.8 2749303.3, 684374.8 2749297.2, 684363.1 2749292.6, 684350.9 2749289.4, " +
      "684338.4 2749287.9, 684325.8 2749287.9, 684313.3 2749289.4, 684301.1 2749292.6, 684289.4 2749297.2, " +
      "684278.4 2749303.3, 684268.2 2749310.7, 684259 2749319.3, 684251 2749329, 684244.3 2749339.6, " +
      "684238.9 2749351, 684235 2749363, 684232.7 2749375.3, 684231.9 2749387.9, 684232.7 2749400.5, " +
      "684233.4 2749404.5))"
    val utm38NPolygon =
      GeomUtils.parseWkt(utm38NPolygonWkt, Coordinates.SaudiArabiaUtmSrid, Coordinates.UtmPrecisionModel)

    val expectedWgs84PolygonWkt = "POLYGON ((46.82329508 24.8484987, 46.82328424 24.84850154, " +
      "46.82279442 24.84870162, 46.82232655 24.84898089, 46.82189541 24.84933645, 46.82151477 24.84976271, " +
      "46.8211984 24.85025319, 46.82095802 24.85079782, 46.82080338 24.85138655, 46.82074419 24.85200661, " +
      "46.82078519 24.85264441, 46.82092913 24.85328457, 46.82117779 24.85391262, 46.82152994 24.85451322, " +
      "46.82197946 24.85507291, 46.82252018 24.85557641, 46.82314203 24.8560112, 46.82383397 24.8563675, " +
      "46.82458199 24.85663372, 46.82537016 24.85680375, 46.82618356 24.85687236, 46.82700531 24.85683612, " +
      "46.82781662 24.85669529, 46.82860071 24.85645276, 46.82934177 24.85611144, 46.83002412 24.85567966, " +
      "46.83063203 24.85516662, 46.83115479 24.8545824, 46.83158071 24.85394069, 46.8319021 24.85325602, " +
      "46.83211422 24.85254199, 46.83221334 24.8518149, 46.83220069 24.85109099, 46.83207747 24.85038378, " +
      "46.83184987 24.84971035, 46.83152599 24.84908325, 46.83111393 24.84851501, 46.83062768 24.84801538, " +
      "46.83007928 24.84759506, 46.82948561 24.84725836, 46.82885961 24.84701052, 46.82821913 24.84685405, " +
      "46.82757897 24.84678695, 46.82695592 24.84680721, 46.82636376 24.84690925, 46.82581524 24.84708568, " +
      "46.82532309 24.84732824, 46.82489704 24.84762506, 46.82489021 24.84763146, 46.82478932 24.84756679, " +
      "46.8246797 24.84751305, 46.82456334 24.84747294, 46.82444223 24.84744553, 46.82431837 24.8474335, " +
      "46.82419373 24.84743503, 46.82407027 24.84745008, 46.82395 24.84748044, 46.82383487 24.84752337, " +
      "46.82372685 24.84757976, 46.82362693 24.84764779, 46.82353705 24.84772653, 46.8234592 24.84781506, " +
      "46.82339432 24.84791155, 46.82334241 24.84801511, 46.82330541 24.8481239, 46.82328429 24.8482352, " +
      "46.82327804 24.84834904, 46.82328762 24.84846267, 46.82329508 24.8484987))"
    val expectedWgs84Polygon =
      GeomUtils.parseWkt(expectedWgs84PolygonWkt, Coordinates.Wgs84GeodeticSrid, Coordinates.LatLongPrecisionModel)

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

  trait WithNearestGeometries {

    val withFixedScaleGeomFactory =
      GeomUtils.geomFactory(Coordinates.SaudiArabiaUtmSrid, Coordinates.UtmPrecisionModel)
    val withPointInsideGeom = withFixedScaleGeomFactory.createPoint(new Coordinate(1, 1))
    val withGeomContainingPoint = withFixedScaleGeomFactory.createPolygon(Array(
      new Coordinate(1, 1),
      new Coordinate(1, 2),
      new Coordinate(2, 2),
      new Coordinate(2, 1),
      new Coordinate(1, 1)))
    val pointWithFixedScale = withFixedScaleGeomFactory.createPoint(new Coordinate(669393.1, 2733929.4))
    val geomWithFixedScale =
      GeomUtils.parseWkt(
        "POLYGON ((669457.4 2734012.4, 669456.7 2734023.2, 669454.7 2734033.8, 669451.4 2734044, 669446.8 2734053.8," +
          " 669441 2734062.9, 669434.1 2734071.2, 669426.3 2734078.6, 669417.5 2734084.9, 669408.1 2734090.1, " +
          "669398 2734094.1, 669387.6 2734096.8, 669376.9 2734098.1, 669366.1 2734098.1, 669355.4 2734096.8, " +
          "669345 2734094.1, 669334.9 2734090.1, 669325.5 2734084.9, 669316.7 2734078.6, 669308.9 2734071.2, " +
          "669302 2734062.9, 669296.2 2734053.8, 669291.6 2734044, 669288.3 2734033.8, 669286.3 2734023.2, " +
          "669285.6 2734012.4, 669286.3 2734001.6, 669288.3 2733991, 669291.6 2733980.8, 669296.2 2733971, " +
          "669302 2733961.9, 669308.9 2733953.6, 669316.7 2733946.2, 669325.5 2733939.9, 669334.9 2733934.7, " +
          "669345 2733930.7, 669355.4 2733928, 669366.1 2733926.7, 669376.9 2733926.7, 669387.6 2733928, " +
          "669398 2733930.7, 669408.1 2733934.7, 669417.5 2733939.9, 669426.3 2733946.2, 669434.1 2733953.6, " +
          "669441 2733961.9, 669446.8 2733971, 669451.4 2733980.8, 669454.7 2733991, 669456.7 2734001.6, " +
          "669457.4 2734012.4))",
        Coordinates.SaudiArabiaUtmSrid,
        Coordinates.UtmPrecisionModel)
    
    val withFloatingGeomFactory =
      GeomUtils.geomFactory(Coordinates.SaudiArabiaUtmSrid, new PrecisionModel(PrecisionModel.FLOATING))
    val pointWithFloating = withFloatingGeomFactory.createPoint(new Coordinate(1.9999, 2))
    val geomWithFloating =
      withFloatingGeomFactory.createPolygon(Array(
        new Coordinate(2, 2),
        new Coordinate(2, 3),
        new Coordinate(3, 3),
        new Coordinate(3, 2),
        new Coordinate(2, 2)))
  }

  "GeomUtils" should "parse WKT text and assign SRID" in new WithShapes {
    GeomUtils.parseWkt(polygonWkt, sridPlanar) should equalGeometry (poly)
  }

  it should "write WKT text (without SRID)" in new WithShapes {
    GeomUtils.wkt(expectedCirSect) should be ("POLYGON ((0 0, 0 -1, 0.4 -0.9, 0.7 -0.7, 0.9 -0.4, 1 0, 0 0))")
  }

  it should "transform a Geometry to another spatial reference system (SRID)" in new WithShapes {
    GeomUtils.transformGeom(
      utm38NPolygon,
      Coordinates.Wgs84GeodeticSrid,
      Coordinates.LatLongPrecisionModel,
      true) should equalGeometry (expectedWgs84Polygon, 1e-7)
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

  it should "ensure a near point gets into a geometry (fixed scale geometry factory)" in new WithNearestGeometries {
    GeomUtils.ensureNearestPointInGeom(pointWithFixedScale, geomWithFixedScale).intersects(geomWithFixedScale) should
      be (true)
  }

  it should "ensure a near point gets into a geometry (floating geometry factory)" in new WithNearestGeometries {
    GeomUtils.ensureNearestPointInGeom(pointWithFloating, geomWithFloating).intersects(geomWithFloating) should be (true)
  }

  it should "ensure a point is contained within a geometry" in new WithNearestGeometries {
    GeomUtils.ensureNearestPointInGeom(withPointInsideGeom, withGeomContainingPoint) should
      equalGeometry(withPointInsideGeom)
  }
}
