/*
 * TODO: License goes here!
 */

package sa.com.mobily.xdr

import org.scalatest.{FlatSpec, ShouldMatchers}

import scala.language.implicitConversions

class CsXdrTest extends FlatSpec with ShouldMatchers {

  import sa.com.mobily.xdr.CsCell._

  trait WithCsCells {

    val csCell =
      CsCell(firstLac = Some("0000"), secondLac = Some("0001"), thirdLac= Some("0002"), oldLac= Some("0003"),
        newLac= Some("0004"))
    val csCellWithoutFirstLac = csCell.copy(firstLac = None)
    val csCellWithoutSecondLac = csCell.copy(secondLac = None)
    val csCellWithoutThirdLac = csCell.copy(thirdLac = None)
    val csCellWithoutOldLac = csCell.copy(oldLac = None)
    val csCellWithoutNewLac = csCell.copy(newLac = None)

    val csCellWithoutValues = CsCell(firstLac = None, secondLac = None, thirdLac= None, oldLac= None, newLac= None)

    val cellHeader = Array("firstLac", "secondLac", "thirdLac", "oldLac", "newLac")

    val cellFields = Array("0000", "0001", "0002", "0003", "0004")
    val cellWithoutFirstLacFields = Array("", "0001", "0002", "0003", "0004")
    val csCellWithoutSecondLacFields = Array("0000", "", "0002", "0003", "0004")
    val csCellWithoutThirdLacFields = Array("0000", "0001", "", "0003", "0004")
    val csCellWithoutOldLacFields = Array("0000", "0001", "0002", "", "0004")
    val csCellWithoutNewLacFields = Array("0000", "0001", "0002", "0003", "")

    val csCellWithoutValuesFields = Array("", "", "", "", "")
  }

  "CsXdr" should "return correct header for Cell" in new WithCsCells {
    CsCell.header should be(cellHeader)
  }

  it should "return correct fields for Cell" in new WithCsCells {
    csCell.fields should be(cellFields)
  }

  it should "return correct fields for Cell when not all attributes are defined" in new WithCsCells {
    csCellWithoutFirstLac.fields should be(cellWithoutFirstLacFields)
    csCellWithoutSecondLac.fields should be(csCellWithoutSecondLacFields)
    csCellWithoutThirdLac.fields should be(csCellWithoutThirdLacFields)
    csCellWithoutOldLac.fields should be(csCellWithoutOldLacFields)
    csCellWithoutNewLac.fields should be(csCellWithoutNewLacFields)
    csCellWithoutValues.fields should be(csCellWithoutValuesFields)
  }
}
