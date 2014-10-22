/*
 * TODO: License goes here!
 */

package sa.com.mobily.cell.spark

import org.scalatest._

import sa.com.mobily.utils.LocalSparkContext

class EgDslTest extends FlatSpec with ShouldMatchers with LocalSparkContext {

  import EgDsl._

  trait WithEgCellsText {

    val egCell1 = "169|7|25154|3361|UDMMEA2515||2|1|3|||0||20|||2|1|3|2515|" +
      "SDE.ST_GEOMETRY(1,1,49.98225,26.59451,49.98225,26.59451,NULL,NULL,NULL,NULL,0,0,4326," +
      "'oracle.sql.BLOB@2ac0290c')|BTS_TECHNOLOGY|2|3G|3G|1|OFF AIR|Off Air||3|3|Hauwei|"
    val egCell2 = "169|7|25154|3361|UDMMEA2515||2|1|3|||NotAzimuth||20|||2|1|3|2515|" +
      "SDE.ST_GEOMETRY(1,1,49.98225,26.59451,49.98225,26.59451,NULL,NULL,NULL,NULL,0,0,4326," +
      "'oracle.sql.BLOB@2ac0290c')|BTS_TECHNOLOGY|2|3G|3G|1|OFF AIR|Off Air||3|3|Hauwei|"
    val egCell3 = "169|7|25154|3361|UDMMEA2515||2|1|3|||0||20|||2|1|3|2515|" +
      "SDE.ST_GEOMETRY(1,1,49.98225,26.59451,49.98225,26.59451,NULL,NULL,NULL,NULL,0,0,4326," +
      "'oracle.sql.BLOB@2ac0290c')|BTS_TECHNOLOGY|2|3G|3G|1|OFF AIR|Off Air||3|3|Hauwei|"

    val egCells = sc.parallelize(List(egCell1, egCell2, egCell3))
  }

  trait WithEgBtsText {
    val egBtsLine1 = "23\t118\t\"2806\"\t\"2806\"\t\"New-Addition\"\t37.34991\t31.31631\t\"\"\t\"\"\t\"3834\"\t" +
      "\"BTS\"\t\"Alcatel\"\t\"East\"\t\"\"\t\"Eastern Pool\"\t\"E317\"\t\"AJREA2806\"\t\"\"\t\"\"\t1\t\"\"\t\"\"\t" +
      "\"\"\t\"\"\t\"42003038342806\"\t\"\"\t9999\t9999\t9999\t9999\t\"\"\t\"\"\t\"2G\"\t17\t\"P3\"\t\"Macro\"\t" +
      "\"Eastern Pool\"\t\"Eastern Pool\"\t\"\"\t\"\"\t1\t10\t1\t1\t4\t619\t27-AUG-14\t535.49793639\t681.54282813\t" +
      "SDE.ST_GEOMETRY(1,1,37.34991,31.31631,37.34991,31.31631,NULL,NULL,NULL,NULL,0,0,4326,'oracle.sql.BLOB@d6bed81')"
    val egBtsLine2 = "23\t118\t\"2806\"\t\"2806\"\t\"New-Addition\"\t37.34991\t31.31631\t\"\"\t\"\"\t\"3834\"\t" +
      "\"BTS\"\t\"Ericsson\"\t\"East\"\t\"\"\t\"Eastern Pool\"\t\"E317\"\t\"AJREA2806\"\t\"\"\t\"\"\t1\t\"\"\t\"\"\t" +
      "\"\"\t\"\"\t\"42003038342806\"\t\"\"\t9999\t9999\t9999\t9999\t\"\"\t\"\"\t\"2G\"\t17\t\"P3\"\t\"Macro\"\t" +
      "\"Eastern Pool\"\t\"Eastern Pool\"\t\"\"\t\"\"\t1\t10\t1\t1\t4\t619\t27-AUG-14\t535.49793639\t681.54282813\t" +
      "SDE.ST_GEOMETRY(1,1,37.34991,31.31631,37.34991,31.31631,NULL,NULL,NULL,NULL,0,0,4326,'oracle.sql.BLOB@d6bed81')"
    val egBtsLine3 = "23\t118\t\"2806\"\t\"2806\"\t\"New-Addition\"\t37.34991\t31.31631\t\"\"\t\"\"\t\"3834\"\t" +
      "\"BTS\"\t\"Alcatel\"\t\"East\"\t\"\"\t\"Eastern Pool\"\t\"E317\"\t\"AJREA2806\"\t\"\"\t\"\"\t1\t\"\"\t\"\"\t" +
      "\"\"\t\"\"\t\"42003038342806\"\t\"\"\t9999\t9999\t9999\t9999\t\"\"\t\"\"\t\"2G\"\t17\t\"P3\"\t\"Macro\"\t" +
      "\"Eastern Pool\"\t\"Eastern Pool\"\t\"\"\t\"\"\t1\t10\t1\t1\t4\t619\t27-AUG-14\t535.49793639\tNotOutdoorCov\t" +
      "SDE.ST_GEOMETRY(1,1,37.34991,31.31631,37.34991,31.31631,NULL,NULL,NULL,NULL,0,0,4326,'oracle.sql.BLOB@d6bed81')"

    val egBtsRdd = sc.parallelize(List(egBtsLine1, egBtsLine2, egBtsLine3))
  }

  "EgContext" should "get correctly parsed EG cells" in new WithEgCellsText {
    egCells.toEgCell.count should be (2)
  }

  it should "get errors when parsing EG cells" in new WithEgCellsText {
    egCells.toEgCellErrors.count should be (1)
  }

  it should "get both correctly and wrongly parsed EG cells" in new WithEgCellsText {
    egCells.toParsedEgCell.count should be (3)
  }

  it should "get correctly parsed EG BTS" in new WithEgBtsText {
    egBtsRdd.toEgBts.count should be (2)
  }

  it should "get errors when parsing EG BTS" in new WithEgBtsText {
    egBtsRdd.toEgBtsErrors.count should be (1)
  }

  it should "get both correctly and wrongly parsed EG BTS" in new WithEgBtsText {
    egBtsRdd.toParsedEgCell.count should be (3)
  }
}
