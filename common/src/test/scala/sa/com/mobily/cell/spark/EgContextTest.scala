/*
 * TODO: License goes here!
 */

package sa.com.mobily.cell.spark

import org.scalatest._
import sa.com.mobily.utils.LocalSparkContext

class EgContextTest extends FlatSpec with ShouldMatchers with LocalSparkContext {

  import EgContext._

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

  "EgContext" should "get correctly parsed EG cells" in new WithEgCellsText {
    egCells.toEgCell.count should be (2)
  }

  it should "get errors when parsing EG cells" in new WithEgCellsText {
    egCells.toEgCellErrors.count should be (1)
  }

  it should "get both correctly and wrongly parsed EG cells" in new WithEgCellsText {
    egCells.toParsedEgCell.count should be (3)
  }
}
