/*
 * TODO: License goes here!
 */

package sa.com.mobily.geometry

import com.vividsolutions.jts.geom.{Coordinate, GeometryCollection, PrecisionModel}
import org.scalatest.{FlatSpec, ShouldMatchers}

import sa.com.mobily.cell.{TwoG, EgBts}
import sa.com.mobily.poi.UserActivity
import sa.com.mobily.utils.EdmCustomMatchers

class GeomUtilsTest extends FlatSpec with ShouldMatchers with EdmCustomMatchers {

  trait WithShapes {

    val sridPlanar = 32638
    val geomFactory = GeomUtils.geomFactory(sridPlanar)

    val polygonWkt = "POLYGON (( 0 0, 1 0, 1 1, 0 1, 0 0 ))"
    val poly = geomFactory.createPolygon(
      Array(new Coordinate(0, 0), new Coordinate(1, 0), new Coordinate(1, 1), new Coordinate(0, 1),
        new Coordinate(0, 0)))
    val polyPoints = Map((1, Map((1, (0D, 0D)), (2, (1D, 0D)), (3, (1D, 1D)), (4, (0D, 1D)), (5, (0D, 0D)))))

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

    val allGeometries =
      List(poly, utm38NPolygon, expectedWgs84Polygon, expectedCircle, expectedCircle, expectedCirSect,
        expectedConchoid, expectedConchoidWithBackLobe, expectedHippopede, expectedHippopedeWithBackLobe)

    val allGeometriesComplex =
      Array(poly, utm38NPolygon,
        new GeometryCollection(Array(expectedWgs84Polygon, expectedCircle, expectedCircle, expectedCirSect,
        expectedConchoid, expectedConchoidWithBackLobe, expectedHippopede, expectedHippopedeWithBackLobe), geomFactory))
  }

  trait WithGeometries {

    val geomWgs84tWkt = "POLYGON ((24.8484987 46.82329508, 24.84850154 46.82328424, 24.84870162 46.82279442, " +
      "24.84898089 46.82232655, 24.84933645 46.82189541, 24.84976271 46.82151477, 24.85025319 46.8211984, " +
      "24.85079782 46.82095802, 24.85138655 46.82080338, 24.85200661 46.82074419, 24.85264441 46.82078519, " +
      "24.85328457 46.82092913, 24.85391262 46.82117779, 24.85451322 46.82152994, 24.85507291 46.82197946, " +
      "24.85557641 46.82252018, 24.8560112 46.82314203, 24.8563675 46.82383397, 24.85663372 46.82458199, " +
      "24.85680375 46.82537016, 24.85687236 46.82618356, 24.85683612 46.82700531, 24.85669529 46.82781662, " +
      "24.85645276 46.82860071, 24.85611144 46.82934177, 24.85567966 46.83002412, 24.85516662 46.83063203, " +
      "24.8545824 46.83115479, 24.85394069 46.83158071, 24.85325602 46.8319021, 24.85254199 46.83211422, " +
      "24.8518149 46.83221334, 24.85109099 46.83220069, 24.85038378 46.83207747, 24.84971035 46.83184987, " +
      "24.84908325 46.83152599, 24.84851501 46.83111393, 24.84801538 46.83062768, 24.84759506 46.83007928, " +
      "24.84725836 46.82948561, 24.84701052 46.82885961, 24.84685405 46.82821913, 24.84678695 46.82757897, " +
      "24.84680721 46.82695592, 24.84690925 46.82636376, 24.84708568 46.82581524, 24.84732824 46.82532309, " +
      "24.84762506 46.82489704, 24.84763146 46.82489021, 24.84756679 46.82478932, 24.84751305 46.8246797, " +
      "24.84747294 46.82456334, 24.84744553 46.82444223, 24.8474335 46.82431837, 24.84743503 46.82419373, " +
      "24.84745008 46.82407027, 24.84748044 46.82395, 24.84752337 46.82383487, 24.84757976 46.82372685, " +
      "24.84764779 46.82362693, 24.84772653 46.82353705, 24.84781506 46.8234592, 24.84791155 46.82339432, " +
      "24.84801511 46.82334241, 24.8481239 46.82330541, 24.8482352 46.82328429, 24.84834904 46.82327804, " +
      "24.84846267 46.82328762, 24.8484987 46.82329508))"
    val geomWgs84 = GeomUtils.parseWkt(geomWgs84tWkt, Coordinates.Wgs84GeodeticSrid, Coordinates.LatLongPrecisionModel)
    val geomWgs84DifferentPrecision = GeomUtils.parseWkt(
      geomWgs84tWkt, Coordinates.Wgs84GeodeticSrid, Coordinates.UtmPrecisionModel)
    val geomFactoryWgs84 = GeomUtils.geomFactory(Coordinates.Wgs84GeodeticSrid, Coordinates.LatLongPrecisionModel)

    val geomWgs84WktLongitudeFirst = "POLYGON ((46.82329508 24.8484987, 46.82328424 24.84850154, " +
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
    val geomWgs84LongitudeFirst = GeomUtils.parseWkt(geomWgs84WktLongitudeFirst,
      Coordinates.Wgs84GeodeticSrid, Coordinates.LatLongPrecisionModel)

    val geomUtm38NWkt = "POLYGON ((684233.4 2749404.5, 684232.3 2749404.8, 684182.5 2749426.3, " +
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
    val geomUtm38N = GeomUtils.parseWkt(geomUtm38NWkt, Coordinates.SaudiArabiaUtmSrid, Coordinates.UtmPrecisionModel)
    val geomFactoryUtm38N = GeomUtils.geomFactory(Coordinates.SaudiArabiaUtmSrid, Coordinates.UtmPrecisionModel)
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

  trait WithIntersectionCells {

    val sridPlanar = 32638
    val geomFactory = GeomUtils.geomFactory(sridPlanar)

    val shapeWkt1 = "POLYGON ((669525.7 2733312.6, 669542 2733322.3, 669590.6 2733341.3, 669637.2 2733352.7, " +
      "669683.2 2733357.9, 669728.6 2733357.4, 669773 2733351.5, 669816.2 2733340.4, 669857.4 2733324.2, " +
      "669896.4 2733303.4, 669932.4 2733278.1, 669965.2 2733248.8, 669994.2 2733215.8, 670019.1 2733179.7, " +
      "670039.5 2733141, 670055.2 2733100.1, 670065.9 2733057.7, 670071.6 2733014.4, 670072.1 2732970.7, " +
      "670067.5 2732927.3, 670057.8 2732884.7, 670043.2 2732843.6, 670023.8 2732804.5, 670000 2732767.9, " +
      "669972 2732734.4, 669940.3 2732704.4, 669905.3 2732678.4, 669867.4 2732656.7, 669827.3 2732639.6, " +
      "669785.4 2732627.3, 669742.3 2732620, 669698.7 2732617.8, 669655.2 2732620.8, 669612.2 2732629, " +
      "669570.5 2732642.1, 669530.7 2732660, 669493.2 2732682.6, 669458.5 2732709.4, 669427.3 2732740.2, " +
      "669399.9 2732774.4, 669376.7 2732811.8, 669358.1 2732851.7, 669344.3 2732893.7, 669335.6 2732937.1, " +
      "669332.1 2732981.5, 669334 2733026.3, 669341.4 2733071.1, 669354.5 2733115.5, 669373.8 2733159.4, " +
      "669400.9 2733204, 669413.2 2733218.3, 669407.4 2733223.8, 669401.5 2733231, 669396.5 2733238.9, " +
      "669392.5 2733247.4, 669389.6 2733256.3, 669387.8 2733265.5, 669387.2 2733274.8, 669387.8 2733284.1, " +
      "669389.6 2733293.3, 669392.5 2733302.2, 669396.5 2733310.7, 669401.5 2733318.6, 669407.4 2733325.8, " +
      "669414.2 2733332.2, 669421.8 2733337.7, 669430 2733342.2, 669438.7 2733345.6, 669447.7 2733347.9, " +
      "669457 2733349.1, 669466.4 2733349.1, 669475.7 2733347.9, 669484.7 2733345.6, 669493.4 2733342.2, " +
      "669501.6 2733337.7, 669509.2 2733332.2, 669516 2733325.8, 669521.9 2733318.6, 669525.7 2733312.6))"
    val shapeWkt2 = "POLYGON ((669357.5 2733346.2, 669357.3 2733346.2, 669291.5 2733364.2, 669231.1 2733389.3, " +
      "669175.1 2733421.4, 669123.8 2733459.8, 669077.9 2733504, 669037.7 2733553.4, 669004 2733607.3, " +
      "668977.1 2733664.8, 668957.3 2733725.1, 668945.1 2733787.3, 668940.6 2733850.5, 668943.7 2733913.8, " +
      "668954.5 2733976.2, 668972.8 2734036.8, 668998.4 2734094.7, 669030.7 2734149.1, 669069.4 2734199.2, " +
      "669114 2734244.2, 669163.6 2734283.4, 669217.6 2734316.4, 669275.3 2734342.5, 669335.6 2734361.4, " +
      "669397.8 2734372.9, 669461 2734376.8, 669524.2 2734372.9, 669586.4 2734361.4, 669646.7 2734342.5, " +
      "669704.4 2734316.4, 669758.4 2734283.4, 669808 2734244.2, 669852.6 2734199.2, 669891.3 2734149.1, " +
      "669923.6 2734094.7, 669949.2 2734036.8, 669967.5 2733976.2, 669978.3 2733913.8, 669981.4 2733850.5, " +
      "669976.9 2733787.3, 669964.7 2733725.1, 669944.9 2733664.8, 669918 2733607.3, 669884.3 2733553.4, " +
      "669844.1 2733504, 669798.2 2733459.8, 669746.9 2733421.4, 669690.9 2733389.3, 669630.5 2733364.2, " +
      "669564.7 2733346.2, 669564.5 2733346.2, 669565.2 2733334.4, 669564.4 2733321.3, 669562 2733308.5, " +
      "669557.9 2733296, 669552.3 2733284.2, 669545.3 2733273.1, 669537 2733263, 669527.4 2733254.1, " +
      "669516.9 2733246.4, 669505.4 2733240.1, 669493.2 2733235.3, 669480.5 2733232, 669467.5 2733230.4, " +
      "669454.5 2733230.4, 669441.5 2733232, 669428.8 2733235.3, 669416.6 2733240.1, 669405.1 2733246.4, " +
      "669394.6 2733254.1, 669385 2733263, 669376.7 2733273.1, 669369.7 2733284.2, 669364.1 2733296, " +
      "669360 2733308.5, 669357.6 2733321.3, 669356.8 2733334.4, 669357.5 2733346.2))"
    val geometry1 = GeomUtils.parseWkt(shapeWkt1, sridPlanar)
    val geometry2 = GeomUtils.parseWkt(shapeWkt2, sridPlanar)
    val rangeTolerance = 1e-6
    val intersectRatio = 0.034577
  }

  trait WithIntersectionShapes {

    val poly1 = GeomUtils.parseWkt("POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))", Coordinates.SaudiArabiaUtmSrid)
    val poly2 = GeomUtils.parseWkt("POLYGON ((0.5 0, 0.5 1, 1.5 1, 1.5 0, 0.5 0))", Coordinates.SaudiArabiaUtmSrid)
    val poly3 = GeomUtils.parseWkt("POLYGON ((3 3, 3 4, 4 4, 4 3, 3 3))", Coordinates.SaudiArabiaUtmSrid)

    val poly1And2Intersection =
      GeomUtils.parseWkt("POLYGON ((0.5 1, 1 1, 1 0, 0.5 0, 0.5 1))", Coordinates.SaudiArabiaUtmSrid)
  }

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
    val nonExistingSiteId = "0"
    val nonExistingRegionId = "0"
    val poiLocation1 = (firstUserSiteId, firstUserRegionId)
    val poiLocation2 = (secondUserSiteId, secondUserRegionId)
    val nonExistingPoiLocation = (nonExistingSiteId, nonExistingRegionId)
    val poisLocation = Seq(poiLocation1, poiLocation2)
    val poisLocationWithoutGeom = Seq(nonExistingPoiLocation)
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
    val shapeWktUnion = "MULTIPOLYGON (((230 0, 2 0, 2 4, 230 4, 230 0)), ((17 14, 130 14, 130 10, 2 10, 2 14, " +
      "16.8 14, 13 114, 17 114, 17 14)))"

    val geom1 = GeomUtils.parseWkt(shapeWkt1 , coords1.srid)
    val geom2 = GeomUtils.parseWkt(shapeWkt2 , coords1.srid)
    val geom3 = GeomUtils.parseWkt(shapeWkt3 , coords1.srid)
    val geom4 = GeomUtils.parseWkt(shapeWkt4 , coords1.srid)
    val geomUnion = GeomUtils.parseWkt(shapeWktUnion , coords1.srid)
    val geoms = Seq(geom1, geom2, geom3, geom4)
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
    GeomUtils.ensureNearestPointInGeom(pointWithFloating, geomWithFloating).intersects(geomWithFloating) should
      be (true)
  }

  it should "ensure a point is contained within a geometry" in new WithNearestGeometries {
    GeomUtils.ensureNearestPointInGeom(withPointInsideGeom, withGeomContainingPoint) should
      equalGeometry(withPointInsideGeom)
  }

  it should "calculate the intersection ratio between two geometries" in new WithIntersectionCells {
    GeomUtils.intersectionRatio(geometry1, geometry2) should be (intersectRatio +- rangeTolerance)
    GeomUtils.intersectionRatio(geometry2, geometry1) should be (intersectRatio +- rangeTolerance)
  }

  it should "return one geometry as a List when asking for simple geometries" in new WithShapes {
    GeomUtils.geomAsSimpleGeometries(poly) should be (List(poly))
    GeomUtils.geomAsSimpleGeometries(utm38NPolygon) should be (List(utm38NPolygon))
    GeomUtils.geomAsSimpleGeometries(expectedWgs84Polygon) should be (List(expectedWgs84Polygon))
    GeomUtils.geomAsSimpleGeometries(expectedCircle) should be (List(expectedCircle))
    GeomUtils.geomAsSimpleGeometries(expectedCirSect) should be (List(expectedCirSect))
    GeomUtils.geomAsSimpleGeometries(expectedConchoid) should be (List(expectedConchoid))
    GeomUtils.geomAsSimpleGeometries(expectedConchoidWithBackLobe) should be (List(expectedConchoidWithBackLobe))
    GeomUtils.geomAsSimpleGeometries(expectedHippopede) should be (List(expectedHippopede))
    GeomUtils.geomAsSimpleGeometries(expectedHippopedeWithBackLobe) should be (List(expectedHippopedeWithBackLobe))
  }

  it should "return all geometries as a List when asking for simple geometries with GeometryCollection" in new
      WithShapes {
    GeomUtils.geomAsSimpleGeometries(new GeometryCollection(allGeometries.toArray, geomFactory)) should be(
      allGeometries)
  }

  it should "return all geometries as a List when asking for simple geometries with GeometryCollections" in new
      WithShapes {
    GeomUtils.geomAsSimpleGeometries(new GeometryCollection(allGeometriesComplex, geomFactory)) should be(
      allGeometries)
  }

  it should "return coordinates as a Map including position" in new WithShapes {
    GeomUtils.geomAsPoints(poly) should be (polyPoints)
  }

  it should "not transform a Geometry when geometry factory is the same UTM 38N and longitudeFirst false" in
      new WithGeometries {
    GeomUtils.transformGeom(geomUtm38N, geomFactoryUtm38N, false) should be (geomUtm38N)
  }

  it should "not transform a Geometry when geometry factory is the same UTM 38N and longitudeFirst true" in
      new WithGeometries {
    GeomUtils.transformGeom(geomUtm38N, geomFactoryUtm38N, true) should be (geomUtm38N)
  }

  it should "not transform a Geometry when geometry factory is the same WGS84 and longitudeFirst false" in
      new WithGeometries {
    GeomUtils.transformGeom(geomWgs84, geomFactoryWgs84, false) should be (geomWgs84)
  }

  it should "not transform a Geometry when geometry factory is the same WGS84 and longitudeFirst true" in
    new WithGeometries {
      GeomUtils.transformGeom(geomWgs84, geomFactoryWgs84, true) should be (geomWgs84)
    }

  it should "transform a Geometry when SRID is not equal to factory provided WGS84 and longitudeFirst false" in
      new WithGeometries {
    GeomUtils.transformGeom(geomUtm38N, geomFactoryWgs84, false) should
      equalGeometry (geomWgs84, Coordinates.SevenDecimalsScale)
  }

  it should "transform a Geometry when SRID is not equal to factory provided WGS84 and longitudeFirst true" in
      new WithGeometries {
    GeomUtils.transformGeom(geomUtm38N, geomFactoryWgs84, true) should
      equalGeometry (geomWgs84LongitudeFirst, Coordinates.SevenDecimalsScale)
  }

  it should "transform a Geometry when precision is not equal to factory provided WGS84 and longitudeFirst false" in
    new WithGeometries {
      GeomUtils.transformGeom(geomWgs84DifferentPrecision, geomFactoryWgs84, false) should
        equalGeometry (geomWgs84, Coordinates.OneDecimalScale)
    }

  it should "transform a Geometry when precision is not equal to factory provided WGS84 and longitudeFirst true" in
    new WithGeometries {
      GeomUtils.transformGeom(geomWgs84DifferentPrecision, geomFactoryWgs84, true) should
        equalGeometry (geomWgs84LongitudeFirst, Coordinates.OneDecimalScale)
    }

  it should "transform a Geometry when SRID is not equal to factory provided UTM 38N and longitudeFirst false" in
      new WithGeometries {
    GeomUtils.transformGeom(geomWgs84, geomFactoryUtm38N, false) should
      equalGeometry (geomUtm38N, Coordinates.OneDecimalScale)
  }

  it should "transform a Geometry when SRID is not equal to factory provided UTM 38N and longitudeFirst true" in
    new WithGeometries {
      GeomUtils.transformGeom(geomWgs84, geomFactoryUtm38N, true) should
        equalGeometry (geomUtm38N, Coordinates.OneDecimalScale)
    }

  it should "find geometries" in new WithUserActivity {
    val geometries = UserActivity.findGeometries(poisLocation, btsCatalogue)
    geometries should be(Seq(geometry1, geometry2))
  }

  it should "return an empty sequence when it does not find a geometry" in new WithUserActivity {
    val geometries = UserActivity.findGeometries(poisLocationWithoutGeom, btsCatalogue)
    geometries should be(Seq.empty)
  }

  it should "union geometries and apply the Douglas Peucker Simplifier" in new WithUnionGeometries {
    GeomUtils.unionGeoms(geoms) should be (geomUnion)
  }

  it should "intersect geometries safely when normal intersection is possible" in new WithUnionGeometries {
    GeomUtils.safeIntersection(geom2, geom4) should be (geom2.intersection(geom4))
  }

  it should "state safely whether two geometries intersect (when normal intersects operation is possible)" in
    new WithUnionGeometries {
      GeomUtils.safeIntersects(geom2, geom4) should be (geom2.intersects(geom4))
    }

  it should "union geometries safely (when normal union is possible)" in new WithUnionGeometries {
    GeomUtils.safeUnion(geom1, geom2) should be (geom1.union(geom2))
  }

  it should "return correct scale as Int for precision model" in new WithShapes {
    utm38NPolygon.getPrecisionModel.getMaximumSignificantDigits should be (2)
    expectedWgs84Polygon.getPrecisionModel.getMaximumSignificantDigits should be (8)
  }
}
