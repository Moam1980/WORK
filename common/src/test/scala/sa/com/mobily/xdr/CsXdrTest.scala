/*
 * TODO: License goes here!
 */

package sa.com.mobily.xdr

import scala.language.implicitConversions

import org.scalatest.{FlatSpec, ShouldMatchers}

import sa.com.mobily.event.{CsIuSource, Event}
import sa.com.mobily.user.User

class CsXdrTest extends FlatSpec with ShouldMatchers {

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

  trait WithEventSubscribers {

    val user1Imsi = "420032275422214"
    val user2Imsi = "12343454545455"
    val user1Msisdn = 4200322L
    val emptyMsisdn = 0L
    val eventUser1 = Event(
      User("", user1Imsi, emptyMsisdn),
      1416156747015L,
      1416156748435L,
      3403,
      33515,
      CsIuSource,
      Some("2"),
      None,
      None,
      None,
      None,
      None)
    val eventUser2 = eventUser1.copy(user = User("", user2Imsi, emptyMsisdn))
    val subscribersCatalogue = Map((user1Imsi, user1Msisdn))
  }

  "CsXdr" should "return correct header for Cell" in new WithCsCells {
    CsCell.Header should be(cellHeader)
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

  it should "fill the user msisdn when the user exists in the subscribers map" in new WithEventSubscribers {
    val filledEventWithMsisdn = CsXdr.fillUserEventWithMsisdn(subscribersCatalogue, eventUser1)
    filledEventWithMsisdn.user.msisdn should be(user1Msisdn)
  }
  
  it should "not fill the user msisdn when the user does not exist in the subscribers map" in new WithEventSubscribers {
    val filledEventWithMsisdn = CsXdr.fillUserEventWithMsisdn(subscribersCatalogue, eventUser2)
    filledEventWithMsisdn.user.msisdn should be(emptyMsisdn)
  }
}
