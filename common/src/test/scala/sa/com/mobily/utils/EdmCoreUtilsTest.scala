/*
 * TODO: License goes here!
 */

package sa.com.mobily.utils

import org.scalatest._

class EdmCoreUtilsTest extends FlatSpec with ShouldMatchers {

  trait WithManyDecimalNumbers {

    val roundDownNumber = 234929.2394829329
    val roundUpNumber = 9037832.592349201

    val roundedDownNumber = 234929.2
    val roundedUpNumber = 9037832.6
  }

  trait WithDates {
    val timestamp = 1412171413000L
    val timestampRoundFLoorHour = 1412168400000L

    val outputDateFormat = "yyyy/MM/dd HH:mm:ss"

    val dateString = "2014/10/01 16:50:13"
  }

  trait WithPhones {
    val phoneNumber = "+966000000000"
    val wrongPhoneNumber = "966000000000"
    val code = 966
    val regionCodes = "SA"

    val spanishPhoneNumber = "+34630000000"
    val spanishCode = 34
    val spanishRegionCodes = "ES"

    val britishPhoneNumber = "+44780000000"
    val britishCode = 44
    val britishRegionCodes = "GB:GG:IM:JE"
  }

  "EdmCoreUtils" should "round numbers down with one decimal" in new WithManyDecimalNumbers {
    EdmCoreUtils.roundAt1(roundDownNumber) should be (roundedDownNumber)
  }

  it should "round numbers up with one decimal" in new WithManyDecimalNumbers {
    EdmCoreUtils.roundAt1(roundUpNumber) should be (roundedUpNumber)
  }

  it should "convert to double" in new WithManyDecimalNumbers {
    EdmCoreUtils.parseDouble("3.14") should be (Some(3.14))
  }

  it should "detect badly formatted doubles" in new WithManyDecimalNumbers {
    EdmCoreUtils.parseDouble("This is not a number") should be (None)
  }

  it should "convert to integer" in new WithManyDecimalNumbers {
    EdmCoreUtils.parseInt("139482") should be (Some(139482))
  }

  it should "detect badly formatted integers" in new WithManyDecimalNumbers {
    EdmCoreUtils.parseInt("3.14") should be (None)
  }

  "EdmCoreUtils" should "return correct string date for timestamp" in new WithDates {
    EdmCoreUtils.parseTimestampToSaudiDate(timestamp) should be (dateString)
  }

  it should "should return correct output format for timestamp" in new WithDates {
    EdmCoreUtils.outputDateTimeFormat should be (outputDateFormat)
  }

  it should "should round timestamp to floor hour" in new WithDates {
    EdmCoreUtils.roundTimestampHourly(timestamp) should be (timestampRoundFLoorHour)
  }

  "EdmCoreUtils" should "return correct country code for saudi phone number" in new WithPhones {
    EdmCoreUtils.getCountryCode(phoneNumber) should be (code)
  }

  it should "throw an exception if the format of the phone number is incorrect" in new WithPhones {
    an [Exception] should be thrownBy EdmCoreUtils.getCountryCode(wrongPhoneNumber)
  }

  it should "return correct region code for saudi phone number" in new WithPhones {
    EdmCoreUtils.getRegionCodesForCountryCode(phoneNumber) should be (regionCodes)
  }

  it should "return correct country code for spanish phone number" in new WithPhones {
    EdmCoreUtils.getCountryCode(spanishPhoneNumber) should be (spanishCode)
  }

  it should "return correct region code for spanish phone number" in new WithPhones {
    EdmCoreUtils.getRegionCodesForCountryCode(spanishPhoneNumber) should be (spanishRegionCodes)
  }

  it should "return correct country code for british phone number" in new WithPhones {
    EdmCoreUtils.getCountryCode(britishPhoneNumber) should be (britishCode)
  }

  it should "return correct region code for british phone number" in new WithPhones {
    EdmCoreUtils.getRegionCodesForCountryCode(britishPhoneNumber) should be (britishRegionCodes)
  }
}
