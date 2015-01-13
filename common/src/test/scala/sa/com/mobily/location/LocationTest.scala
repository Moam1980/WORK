/*
 * TODO: License goes here!
 */

package sa.com.mobily.location

import com.vividsolutions.jts.geom.PrecisionModel
import org.scalatest.{FlatSpec, ShouldMatchers}

import sa.com.mobily.geometry.GeomUtils
import sa.com.mobily.parsing.CsvParser

class LocationTest extends FlatSpec with ShouldMatchers {

  import Location._

  trait WithClient {

    val client = Client(id = 0, name = "clientTest")
    val clientEqual = Client(id = 0, name = "clientTestChanged")
    val clientDifferent = Client(id = 1, name = "clientTest")

    val clientHeader = Array("id", "name")
    val clientFields = Array("0", "clientTest")
    val clientEqualFields = Array("0", "clientTestChanged")
    val clientDifferentFields = Array("1", "clientTest")
    val clientHashCode = client.id.hashCode
  }

  trait WithLocation extends WithClient {

    val epsgUtm38N = "EPSG:32638"
    val precisionUtm38N = 10d
    val shapeWkt = "POLYGON ((684233.4 2749404.5, 684232.3 2749404.8, 684182.5 2749426.3, " +
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
    val geom = GeomUtils.parseWkt(shapeWkt, 32638, new PrecisionModel(precisionUtm38N))

    val epsgWgs84 = "EPSG:4326"
    val precisionWgs84 = 1e7d
    val shapeWgs84Wkt = "POLYGON ((46.82329508 24.8484987, 46.82328424 24.84850154, " +
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
    val geomWsg84 = GeomUtils.parseWkt(shapeWgs84Wkt, 4326, new PrecisionModel(precisionWgs84))

    val locationLine = "\"0\"|\"locationTest\"|\"0\"|\"clientTest\"|\"" + epsgWgs84 + "\"|\"" +
      precisionWgs84.toString + "\"|\"" + shapeWgs84Wkt + "\""
    val location = Location(
      id = 0,
      name = "locationTest",
      client = client,
      epsg = epsgWgs84,
      precision = precisionWgs84,
      geomWkt = shapeWgs84Wkt)

    val locationEqualLine = "\"0\"|\"locationEqualTest\"|\"0\"|\"clientTestChanged\"|\"" + epsgUtm38N + "\"|\"" +
      precisionUtm38N.toString + "\"|\"" + shapeWkt + "\""
    val locationEqual = Location(
      id = 0,
      name = "locationEqualTest",
      client = clientEqual,
      epsg = epsgUtm38N,
      precision = precisionUtm38N,
      geomWkt = shapeWkt)

    val locationDifferentLine = "\"1\"|\"locationTest\"|\"0\"|\"clientTest\"|\"" + epsgWgs84 + "\"|\"" +
      precisionWgs84.toString + "\"|\"" + shapeWgs84Wkt + "\""
    val locationDifferent = location.copy(id = 1)

    val locationDifferentClientLine = "\"0\"|\"locationTest\"|\"1\"|\"clientTest\"|\"" + epsgWgs84 + "\"|\"" +
      precisionWgs84.toString + "\"|\"" + shapeWgs84Wkt + "\""
    val locationDifferentClient = location.copy(client = location.client.copy(id = 1))

    val locationHeader = Array("id", "name", "id", "name", "epsg", "precision", "geomWkt")
    val locationFields =
      Array("0", "locationTest", "0", "clientTest", epsgWgs84, precisionWgs84.toString, shapeWgs84Wkt)
    val locationEqualFields =
      Array("0", "locationEqualTest", "0", "clientTestChanged", epsgUtm38N, precisionUtm38N.toString, shapeWkt)
    val locationDifferentFields =
      Array("1", "locationTest", "0", "clientTest", epsgWgs84, precisionWgs84.toString, shapeWgs84Wkt)
    val locationHashCode = location.id.hashCode
  }

  "Client" should "return correct header" in new WithClient {
    Client.header should be (clientHeader)
  }

  it should "return correct fields" in new WithClient {
    client.fields should be (clientFields)
    clientEqual.fields should be (clientEqualFields)
    clientDifferent.fields should be (clientDifferentFields)
  }

  it should "return correct hashCode" in new WithClient {
    client.hashCode should be (clientHashCode)
  }

  it should "return true when comparing with same id" in new WithClient {
    client.equals(clientEqual) should be (true)
    client == clientEqual should be (true)
  }

  it should "return false when comparing with different id" in new WithClient {
    client.equals(clientDifferent) should be (false)
    client == clientDifferent should be (false)

    clientEqual.equals(clientDifferent) should be (false)
    clientEqual == clientDifferent should be (false)
  }

  it should "return false when comparing different objects" in new WithClient {
    client.equals(0) should be (false)
    client == 0 should be (false)
  }

  "Location" should "return correct header" in new WithLocation {
    Location.header should be (locationHeader)
  }

  it should "return correct fields" in new WithLocation {
    location.fields should be (locationFields)
    locationEqual.fields should be (locationEqualFields)
    locationDifferent.fields should be (locationDifferentFields)
  }

  it should "return correct hashCode" in new WithLocation {
    location.hashCode should be (locationHashCode)
  }

  it should "return true when comparing with same id" in new WithLocation {
    location.equals(locationEqual) should be (true)
    location == locationEqual should be (true)
  }

  it should "return false when comparing with different id" in new WithLocation {
    location.equals(locationDifferent) should be (false)
    location == locationDifferent should be (false)

    locationEqual.equals(locationDifferent) should be (false)
    locationEqual == locationDifferent should be (false)
  }

  it should "return false when comparing different objects" in new WithLocation {
    location.equals(client) should be (false)
    location == client should be (false)
  }

  it should "return correct srid" in new WithLocation {
    location.srid should be (4326)
    locationEqual.srid should be (32638)
  }

  it should "return correct geometry" in new WithLocation {
    location.geom should be (geomWsg84)
    locationEqual.geom should be (geom)
  }

  it should "be built from CSV" in new WithLocation {
    // as we have override equals we should check all fields
    CsvParser.fromLine(locationLine).value.get.fields should be (location.fields)
    CsvParser.fromLine(locationEqualLine).value.get.fields should be (locationEqual.fields)
    CsvParser.fromLine(locationDifferentLine).value.get.fields should be (locationDifferent.fields)
    CsvParser.fromLine(locationDifferentClientLine).value.get.fields should be (locationDifferentClient.fields)

    // Just in case something is removed from fields method
    val locationParsed = CsvParser.fromLine(locationLine).value.get
    locationParsed.id should be (location.id)
    locationParsed.name should be (location.name)
    locationParsed.client.id should be (location.client.id)
    locationParsed.client.name should be (location.client.name)
    locationParsed.epsg should be (location.epsg)
    locationParsed.geomWkt should be (location.geomWkt)
  }

  it should "be discarded when the CSV format is wrong" in new WithLocation {
    an [Exception] should be thrownBy fromCsv.fromFields(location.fields.updated(0, "WrongNumber"))
  }
}
