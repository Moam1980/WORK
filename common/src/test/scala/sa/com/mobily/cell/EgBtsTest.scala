/*
 * TODO: License goes here!
 */

package sa.com.mobily.cell

import org.scalatest._

import sa.com.mobily.geometry.{Coordinates, GeomUtils, UtmCoordinates}
import sa.com.mobily.parsing.CsvParser

class EgBtsTest extends FlatSpec with ShouldMatchers {

  import EgBts._

  trait WithEgBts {

    val egBtsLine = ",,13,61,9999,9999,,3870,Macro,8,42003038353870,3870,11/16/2014 9:03:08 AM,,,Eastern Pool," +
      "AN NUQAYRAH,161,48,,,1511.99417334,3835,27.86804,,48.26362,,East,Eastern Pool,ASR3870,,7,,1924.35622061,," +
      "E379,Eastern Pool,P3,4,1,New-Addition,1,2G,1,BTS,,Alcatel,1"
    val fields = Array("", "", "13", "61", "9999", "9999", "", "3870", "Macro", "8", "42003038353870", "3870",
      "11/16/2014 9:03:08 AM", "", "", "Eastern Pool", "AN NUQAYRAH", "161", "48", "", "", "1511.99417334", "3835",
      "27.86804", "", "48.26362", "", "East", "Eastern Pool", "ASR3870", "", "7", "", "1924.35622061", "", "E379",
      "Eastern Pool", "P3", "4", "1", "New-Addition", "1", "2G", "1", "BTS", "", "Alcatel", "1")
    val coords = UtmCoordinates(821375.9, 3086866.0)
    val egBts = EgBts("3870", "3870", "New-Addition", coords, "", "", 3835, "BTS", "Alcatel", "E379",
      "42003038353870", TwoG, "48", "Macro", 1511.99417334, 1924.35622061)
    val polygonWkt = "POLYGON ((1500 0, 1488.2 188, 1452.9 373, 1394.7 552.2, 1314.5 722.6, 1213.5 881.7, 1093.5 " +
      "1026.8, 956.1 1155.8,803.7 1266.5, 638.7 1357.2, 463.5 1426.6, 281.1 1473.4, 94.2 1497, -94.2 1497, -281.1 " +
      "1473.4, -463.5 1426.6, -638.7 1357.2, -803.7 1266.5, -956.1 1155.8, -1093.5 1026.8, -1213.5 881.7, -1314.5 " +
      "722.6, -1394.7 552.2, -1452.9 373, -1488.2 188, -1500 0, -1488.2 -188, -1452.9 -373, -1394.7 -552.2, -1314.5 " +
      "-722.6, -1213.5 -881.7, -1093.5 -1026.8, -956.1 -1155.8, -803.7 -1266.5, -638.7 -1357.2, -463.5 -1426.6, -281.1" +
      " -1473.4, -94.2 -1497, 94.2 -1497, 281.1 -1473.4, 463.5 -1426.6, 638.7 -1357.2, 803.7 -1266.5, 956.1 -1155.8," +
      " 1093.5 -1026.8, 1213.5 -881.7, 1314.5 -722.6, 1394.7 -552.2, 1452.9 -373, 1488.2 -188, 1500 0))"
  }

  "EgBts" should "be built from CSV" in new WithEgBts {
    CsvParser.fromLine(egBtsLine).value.get should be (egBts)
  }

  it should "be discarded when the CSV format is wrong" in new WithEgBts {
    an [Exception] should be thrownBy fromCsv.fromFields(fields.updated(25, "NotLongitude"))
  }

  it should "build circle geometry from centre and outdoor coverage range" in new WithEgBts {
    egBts.copy(coords = UtmCoordinates(0, 0), outdoorCov = 1000).geom should
      be (GeomUtils.parseWkt(polygonWkt, Coordinates.SaudiArabiaUtmSrid))
  }
}
