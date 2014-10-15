/*
 * TODO: License goes here!
 */

package sa.com.mobily.cell

import org.scalatest.{FlatSpec, ShouldMatchers}
import sa.com.mobily.parsing.CsvParser

class EgCellTest extends FlatSpec with ShouldMatchers {

  import EgCell._

  trait WithEgCell {
    val egCellLine = "169|7|25154|3361|UDMMEA2515||2|1|3|||0||20|||2|1|3|2515|" +
      "SDE.ST_GEOMETRY(1,1,49.98225,26.59451,49.98225,26.59451,NULL,NULL,NULL,NULL,0,0,4326," +
      "'oracle.sql.BLOB@2ac0290c')|BTS_TECHNOLOGY|2|3G|3G|1|OFF AIR|Off Air||3|3|Hauwei|"
    val fields = Array("169", "7", "25154", "3361", "UDMMEA2515", "", "2", "1", "3", "", "", "0", "", "20",
      "", "", "2", "1", "3", "2515",
      "SDE.ST_GEOMETRY(1,1,49.98225,26.59451,49.98225,26.59451,NULL,NULL,NULL,NULL,0,0,4326," +
        "'oracle.sql.BLOB@2ac0290c')", "BTS_TECHNOLOGY", "2", "3G", "3G", "1", "OFF", "AIR", "Off Air",
      "", "3", "3", "Hauwei")
    val egCell = EgCell("2515", "25154", 0.0, 20.0, "3361", "UDMMEA2515", "", "", "", "", "", "1", 0.0, "2",
      "SDE.ST_GEOMETRY(1,1,49.98225,26.59451,49.98225,26.59451,NULL,NULL,NULL,NULL,0,0,4326," +
        "'oracle.sql.BLOB@2ac0290c')", 26.59451, 49.98225,
      EgCellCodeDesc("3", "BTS_TECHNOLOGY", "2", "3G", "3G", "1", "OFF AIR", "Off Air", "", "3", "3", "Hauwei", ""))
  }

  "EgCell" should "be built from CSV" in new WithEgCell {
    CsvParser.fromLine(egCellLine).value.get should be (egCell)
  }

  it should "be discarded when the CSV format is wrong" in new WithEgCell {
    an [Exception] should be thrownBy fromCsv.fromFields(fields.updated(11, "NotAzimuth"))
  }
}
