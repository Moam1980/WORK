/*
 * TODO: License goes here!
 */

package sa.com.mobily.cell

import org.scalatest._

import sa.com.mobily.geometry.UtmCoordinates
import sa.com.mobily.parsing.CsvParser

class EgBtsTest extends FlatSpec with ShouldMatchers {

  import EgBts._

  trait WithEgBts {
    val egBtsLine = "23\t118\t\"2806\"\t\"2806\"\t\"New-Addition\"\t37.34991\t31.31631\t\"\"\t\"\"\t\"3834\"\t" +
      "\"BTS\"\t\"Alcatel\"\t\"East\"\t\"\"\t\"Eastern Pool\"\t\"E317\"\t\"AJREA2806\"\t\"\"\t\"\"\t1\t\"\"\t\"\"\t" +
      "\"\"\t\"\"\t\"42003038342806\"\t\"\"\t9999\t9999\t9999\t9999\t\"\"\t\"\"\t\"2G\"\t17\t\"P3\"\t\"Macro\"\t" +
      "\"Eastern Pool\"\t\"Eastern Pool\"\t\"\"\t\"\"\t1\t10\t1\t1\t4\t619\t27-AUG-14\t535.49793639\t681.54282813\t" +
      "SDE.ST_GEOMETRY(1,1,37.34991,31.31631,37.34991,31.31631,NULL,NULL,NULL,NULL,0,0,4326,'oracle.sql.BLOB@d6bed81')"
    val fields = Array("23", "118", "\"2806\"", "\"2806\"", "\"New-Addition\"", "37.34991", "31.31631", "\"\"",
      "\"\"", "\"3834\"", "\"BTS\"", "\"Alcatel\"", "\"East\"", "\"\"", "\"Eastern Pool\"", "\"E317\"",
      "\"AJREA2806\"", "\"\"", "\"\"", "1", "\"\"", "\"\"", "\"\"", "\"42003038342806\"", "\"\"", "9999", "9999",
      "9999", "9999", "\"\"", "\"\"", "\"2G\"", "17", "\"P3\"", "\"Macro\"", "\"Eastern Pool\"", "\"Eastern Pool\"",
      "\"\"", "\"\"", "1", "10", "1", "1", "4", "619", "27-AUG-14", "535.49793639", "681.54282813",
      "SDE.ST_GEOMETRY(1,1,37.34991,31.31631,37.34991,31.31631,NULL,NULL,NULL,NULL,0,0,4326,'oracle.sql.BLOB@d6bed81')")
    val coords = UtmCoordinates(-228902.5, 3490044.0)
    val egBts = EgBts("2806", "2806", "New-Addition", coords, "", "", 3834, "BTS", "Alcatel", "E317",
      "42003038342806", TwoG, "17", "Macro", 535.49793639, 681.54282813)
  }
  
  "EgBts" should "be built from CSV" in new WithEgBts {
    CsvParser.fromLine(egBtsLine).value.get should be (egBts)
  }

  it should "be discarded when the CSV format is wrong" in new WithEgBts {
    an [Exception] should be thrownBy fromCsv.fromFields(fields.updated(7, "NotLongitude"))
  }
}
