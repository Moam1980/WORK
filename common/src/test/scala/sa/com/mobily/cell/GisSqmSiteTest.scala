/*
 * TODO: License goes here!
 */

package sa.com.mobily.cell

import org.scalatest.{ShouldMatchers, FlatSpec}

import sa.com.mobily.geometry.UtmCoordinates
import sa.com.mobily.parsing.CsvParser

class GisSqmSiteTest extends FlatSpec with ShouldMatchers {

  import GisSqmSite._

  trait WithGisSqmSite {
    val gisSqmSiteLine = "420030145704439|4439|4439|AL FAYSALIYYAH AL JANUBIYYAH|Tabouk|TABUK|TABUK|14570|" +
      "28.3899|36.5815|NSN|4G_FDD|Macro|23|M2P5 and  M2P6 and  M2P7|North|LTE|P3||Western Pool|" +
      "Western Pool|||New-Addition|||Tabook"
    val fields = Array("420030145704439", "4439", "4439", "AL FAYSALIYYAH AL JANUBIYYAH", "Tabouk", "TABUK",
      "TABUK", "14570", "28.3899", "36.5815", "NSN", "4G_FDD", "Macro", "23", "M2P5 and  M2P6 and  M2P7", "North",
      "LTE", "P3", "", "Western Pool", "Western Pool", "", "", "New-Addition", "", "", "Tabook")
    val gisSqmSite = GisSqmSite("420030145704439", "4439", "4439", "AL FAYSALIYYAH AL JANUBIYYAH", "Tabouk", "TABUK",
      "TABUK", "14570", UtmCoordinates(-326367.0, 3169394.8, "EPSG:32638"), "NSN", FourGFdd, Macro, 23.0,
      "M2P5 and  M2P6 and  M2P7", "North", "LTE", "P3", PoolInfo("", "Western Pool", "Western Pool", "", ""),
      "New-Addition", "", "", "Tabook")
  }

  "GisSqmSite" should "be built from CSV" in new WithGisSqmSite {
    CsvParser.fromLine(gisSqmSiteLine).value.get should be (gisSqmSite)
  }

  it should "be discarded when the CSV format is wrong" in new WithGisSqmSite {
    an [Exception] should be thrownBy fromCsv.fromFields(fields.updated(13, "WrongType"))
  }
}
