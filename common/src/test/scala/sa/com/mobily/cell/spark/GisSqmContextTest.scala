/*
 * TODO: License goes here!
 */

package sa.com.mobily.cell.spark

import org.scalatest._
import sa.com.mobily.utils.LocalSparkContext

class GisSqmContextTest extends FlatSpec with ShouldMatchers with LocalSparkContext {

  import GisSqmContext._

  trait WithCellsText {

    val gisSqmCell1 = "420030105201811|01|181A|42003010520181|102|K742266|47|900|BTS|2G|Macro|33"
    val gisSqmCell2 = "420030101201911|01|181A|42003010520181|102|K742266|47|900|BTS|3G|Macro|33"
    val gisSqmCell3 = "420030105201811|01|181A|42003010520181|102|K742266|47|900|BTS|2G|TV|33"
    val gisSqmSite1 =
      "42003017056454|6454|AJ6454||Al-Kharj|AR RIYADH|AL AFLAJ|1705|21.4389|46.12195|Huawei|2G|MACRO|" +
        "58|E134|Central|BTS|P3|Riyadh Pool|Central Pool|Central Pool|||New-Addition|AL AFLAJ-AS SULAYYIL Road||"
    val gisSqmSite2 =
      "42003017056454|6454|AJ6454||Al-Kharj|AR RIYADH|AL AFLAJ|1705|21.4389||Huawei|2G|MACRO|" +
        "58|E134|Central|BTS|P3|Riyadh Pool|Central Pool|Central Pool|||New-Addition|AL AFLAJ-AS SULAYYIL Road||"
    val gisSqmSite3 =
      "42003017056454|6454|AJ6454||Al-Kharj|AR RIYADH|AL AFLAJ|1705|21.4389|46.12195|Huawei|G|MACRO|" +
        "58|E134|Central|BTS|P3|Riyadh Pool|Central Pool|Central Pool|||New-Addition|AL AFLAJ-AS SULAYYIL Road||"

    val gisSqmCells = sc.parallelize(List(gisSqmCell1, gisSqmCell2, gisSqmCell3))
    val gisSqmSites = sc.parallelize(List(gisSqmSite1, gisSqmSite2, gisSqmSite3))
  }

  "GisSqmContext" should "get correctly parsed GIS SQM cells" in new WithCellsText {
    gisSqmCells.toGisSqmCell.count should be (2)
  }

  it should "get errors when parsing GIS SQM cells" in new WithCellsText {
    gisSqmCells.toGisSqmCellErrors.count should be (1)
  }

  it should "get both correctly and wrongly parsed GIS SQM cells" in new WithCellsText {
    gisSqmCells.toParsedGisSqmCell.count should be (3)
  }

  it should "get correctly parsed GIS SQM sites" in new WithCellsText {
    gisSqmSites.toGisSqmSite.count should be (1)
  }

  it should "get errors when parsing GIS SQM sites" in new WithCellsText {
    gisSqmSites.toGisSqmSiteErrors.count should be (2)
  }

  it should "get both correctly and wrongly parsed GIS SQM sites" in new WithCellsText {
    gisSqmSites.toParsedGisSqmSite.count should be (3)
  }
}
