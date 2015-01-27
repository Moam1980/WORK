/*
 * TODO: License goes here!
 */

package sa.com.mobily.cell.spark

import org.scalatest._

import sa.com.mobily.cell.{FourGTdd, ThreeG, EgBts}
import sa.com.mobily.geometry.UtmCoordinates
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
    val egBtsLine1 = ",,11,97,3581,3581057,,2181,MACRO,5,42003021752181,2181,11/16/2014 9:03:08 AM,,," +
      "Western Pool,,618,23,,,461.04367785,2175,21.53853,,39.19721,,West,Jeddah Pool,UXJD2181,,14,," +
      "658.63382551,,N212,Western Pool,P1,2,3,New-Addition,1,3G,2,Node B,,Ericsson,2"
    val egBtsLine2 = ",,11,97,3581,3581027,,2183,MACRO,5,42003021752183,2183,11/16/2014 9:03:08 AM,,," +
      "Western Pool,,618,23.5,,,471.0663665,2175,21.5554,,39.20935,,West,Jeddah Pool,UXJD2183,,15,," +
      "672.95195215,,N212,Western Pool,P2,3,3,New-Addition,1,3G,2,Node B,,Ericsson,2"
    val egBtsLine3 = ",,11,97,3581,3581026,,2115,MACRO,5,42003021762115,2115,11/16/2014 9:03:08 AM,,," +
      "Western Pool,,618,17.5,,,350.79410272,2176,21.58388,,39.15818,,West,Jeddah Pool,UXJD2115,,18,," +
      "NotOutdoorCov,,N211,Western Pool,P1,2,3,New-Addition,1,3G,2,Node B,,Ericsson,2"

    val egBtsRdd = sc.parallelize(List(egBtsLine1, egBtsLine2, egBtsLine3))
  }

  trait WithEgBts {

    val egBts1 =
      EgBts(
        "2181",
        "2181",
        "New-Addition",
        UtmCoordinates(-101643.8, 2392961.7, "EPSG:32638"),
        "",
        "",
        2175,
        "Node B",
        "Ericsson",
        "N212",
        "42003021752181",
        ThreeG,
        "23",
        "MACRO",
        461.04367785,
        658.63382551)
    val egBts2 =
      EgBts(
        "2183",
        "2183",
        "New-Addition",
        UtmCoordinates(-100312.1, 2394788.9, "EPSG:32638"),
        "",
        "",
        3184,
        "Node B",
        "Ericsson",
        "N212",
        "42003021752183",
        ThreeG,
        "23.5",
        "MACRO",
        471.0663665,
        672.95195215)
    val egBts3 = egBts2.copy(lac = 3182, technology = FourGTdd)

    val egBts = sc.parallelize(Array(egBts1, egBts2, egBts3))
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

  it should "broadcast the BTS with (bts, regionId) as key" in new WithEgBts {
    val egBtsMap = egBts.toBroadcastMapWithRegion.value
    egBtsMap.size should be (2)
    egBtsMap(("2181", "2")) should be (Iterable(egBts1))
    egBtsMap(("2183", "3")) should be (Iterable(egBts2, egBts3))
  }
}
