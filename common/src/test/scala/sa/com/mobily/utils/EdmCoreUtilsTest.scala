/*
 * TODO: License goes here!
 */

package sa.com.mobily.utils

import org.joda.time.format.DateTimeFormat
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
    val timestampRoundFLoorDay = 1412121600000L

    val outputDateFormat = "yyyy/MM/dd HH:mm:ss"

    val dateString = "2014/10/01 16:50:13"
    val dateTimePatternWithZone = DateTimeFormat.forPattern("yyyymmdd").withZone(EdmCoreUtils.TimeZoneSaudiArabia)
    val firstSaturdayOfTheYear = dateTimePatternWithZone.parseDateTime("20150103")
    val firstSundayOfTheYear = dateTimePatternWithZone.parseDateTime("20150104")
  }

  trait WithPhones {

    val phoneNumber = "+966000000000"
    val wrongPhoneNumber = "966000000000"
    val saudiCode = 966
    val saudiRegionCodesList = List("SA")
    val saudiRegionCodes = "SA"

    val spanishPhoneNumber = "+34630000000"
    val spanishCode = 34
    val spanishRegionCodesList = List("ES")
    val spanishRegionCodes = "ES"

    val britishPhoneNumber = "+44780000000"
    val britishCode = 44
    val britishRegionCodesList = List("GB", "GG", "IM", "JE")
    val britishRegionCodes = "GB:GG:IM:JE"
  }

  "EdmCoreUtils" should "round numbers down with one decimal" in new WithManyDecimalNumbers {
    EdmCoreUtils.roundAt1(roundDownNumber) should be (roundedDownNumber)
  }

  it should "round numbers up with one decimal" in new WithManyDecimalNumbers {
    EdmCoreUtils.roundAt1(roundUpNumber) should be (roundedUpNumber)
  }

  it should "convert to double" in {
    EdmCoreUtils.parseDouble("3.14") should be (Some(3.14))
  }

  it should "detect badly formatted doubles" in {
    EdmCoreUtils.parseDouble("This is not a number") should be (None)
  }

  it should "convert to integer" in {
    EdmCoreUtils.parseInt("139482") should be (Some(139482))
  }

  it should "detect badly formatted integers" in {
    EdmCoreUtils.parseInt("3.14") should be (None)
  }

  it should "convert to long" in {
    EdmCoreUtils.parseLong("139482") should be (Some(139482L))
  }

  it should "detect badly formatted long" in {
    EdmCoreUtils.parseLong("3.14") should be (None)
  }

  it should "convert to float" in {
    EdmCoreUtils.parseFloat("3.14") should be (Some(3.14F))
  }

  it should "detect badly formatted float" in {
    EdmCoreUtils.parseFloat("This is not a number") should be (None)
  }

  it should "convert string hexadecimal to decimal" in {
    EdmCoreUtils.hexToDecimal("F") should be (Some(15))
  }

  it should "detect badly formatted hexadecimal" in {
    EdmCoreUtils.hexToDecimal("GAF2") should be (None)
  }

  it should "convert string hexadecimal to short" in {
    EdmCoreUtils.hexToShort("B") should be (Some(11))
  }

  it should "detect badly short overflows" in {
    EdmCoreUtils.hexToShort("FFA98FFFFA9") should be (None)
  }

  it should "convert to short" in {
    EdmCoreUtils.parseShort("1") should be (Some(1))
  }

  it should "detect badly formatted short" in {
    EdmCoreUtils.parseShort("This is not a number") should be (None)
  }

  it should "convert Any to double" in {
    EdmCoreUtils.doubleOption(3.14D) should be (Some(3.14))
  }

  it should "not convert non-double Any to double" in {
    EdmCoreUtils.doubleOption('A') should be (None)
  }

  it should "convert Any to integer" in {
    EdmCoreUtils.intOption(139482) should be (Some(139482))
  }

  it should "not convert decimal numerals to integer" in {
    EdmCoreUtils.intOption(3.14) should be (None)
  }

  it should "convert Any to long" in {
    EdmCoreUtils.longOption(139482L) should be (Some(139482L))
  }

  it should "not convert decimal numerals to long" in {
    EdmCoreUtils.longOption(3.14) should be (None)
  }

  it should "convert Any to float" in {
    EdmCoreUtils.floatOption(3.14F) should be (Some(3.14F))
  }

  it should "not convert non-float Any to float" in {
    EdmCoreUtils.floatOption('A') should be (None)
  }

  it should "convert Any to short" in {
    EdmCoreUtils.shortOption(1.toShort) should be (Some(1))
  }

  it should "not convert non-short Any to short" in {
    EdmCoreUtils.shortOption('A') should be (None)
  }

  it should "convert Any to string" in {
    EdmCoreUtils.stringOption("A") should be (Some("A"))
  }

  it should "not convert non-text Any to short" in {
    EdmCoreUtils.stringOption(1) should be (None)
  }

  it should "convert Any to integer or zero" in {
    EdmCoreUtils.intOrZero(139482) should be (139482)
  }

  it should "not convert decimal numerals to integer when intOrZero returning zero" in {
    EdmCoreUtils.intOrZero(3.14) should be (0)
  }

  it should "return zero intOrZero when is empty" in {
    EdmCoreUtils.intOrZero(None) should be (0)
  }

  it should "convert Any to long when intOrZero" in {
    EdmCoreUtils.longOrZero(139482L) should be (139482L)
  }

  it should "not convert decimal numerals to long when intOrZero returning zero" in {
    EdmCoreUtils.longOrZero(3.14) should be (0L)
  }

  it should "return zero intOrZero when is none" in {
    EdmCoreUtils.longOrZero(None) should be (0L)
  }

  it should "validate MSISDN is zero" in {
    EdmCoreUtils.validMsisdn("00") should be (None)
  }

  it should "validate MSISDN is not zero" in {
    EdmCoreUtils.validMsisdn("1234567890000") should be (Some(1234567890000L))
  }

  it should "return correct string date for timestamp" in new WithDates {
    EdmCoreUtils.parseTimestampToSaudiDate(timestamp) should be (dateString)
  }

  it should "should return correct output format for timestamp" in new WithDates {
    EdmCoreUtils.outputDateTimeFormat should be (outputDateFormat)
  }

  it should "should round timestamp to floor hour" in new WithDates {
    EdmCoreUtils.roundTimestampHourly(timestamp) should be (timestampRoundFLoorHour)
  }

  it should "should round timestamp to floor day" in new WithDates {
    EdmCoreUtils.roundTimestampDaily(timestamp) should be (timestampRoundFLoorDay)
  }

  it should "return correct country code for saudi phone number" in new WithPhones {
    EdmCoreUtils.getCountryCode(phoneNumber) should be (saudiCode)
  }

  it should "throw an exception if the format of the phone number is incorrect" in new WithPhones {
    an [Exception] should be thrownBy EdmCoreUtils.getCountryCode(wrongPhoneNumber)
  }

  it should "return correct region code for saudi phone number" in new WithPhones {
    EdmCoreUtils.getRegionCodesForCountryCode(phoneNumber) should be (saudiRegionCodes)
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

  it should "return correct region code list for saudi phone number" in new WithPhones {
    EdmCoreUtils.getRegionCodesForCountryCodeList(phoneNumber) should be (saudiRegionCodesList)
  }

  it should "return correct region code list for spanish phone number" in new WithPhones {
    EdmCoreUtils.getRegionCodesForCountryCodeList(spanishPhoneNumber) should be (spanishRegionCodesList)
  }

  it should "return correct region code list for british phone number" in new WithPhones {
    EdmCoreUtils.getRegionCodesForCountryCodeList(britishPhoneNumber) should be (britishRegionCodesList)
  }

  it should "return correct country calling code for saudi phone number" in new WithPhones {
    EdmCoreUtils.getCountryCallingCode(phoneNumber.toLong) should be (saudiCode)
  }

  it should "return correct country calling code for spain phone number" in new WithPhones {
    EdmCoreUtils.getCountryCallingCode(spanishPhoneNumber.toLong) should be (spanishCode)
  }

  it should "return correct country calling code for british phone number" in new WithPhones {
    EdmCoreUtils.getCountryCallingCode(britishPhoneNumber.toLong) should be (britishCode)
  }

  it should "return true when string Y when parseYesNoBooleanOption" in new WithPhones {
    EdmCoreUtils.parseYesNoBooleanOption("Y") should be (Some(true))
  }

  it should "return true when string y when parseYesNoBooleanOption" in new WithPhones {
    EdmCoreUtils.parseYesNoBooleanOption("y") should be (Some(true))
  }

  it should "return true when string YES when parseYesNoBooleanOption" in new WithPhones {
    EdmCoreUtils.parseYesNoBooleanOption("YES") should be (Some(true))
  }

  it should "return true when string yes when parseYesNoBooleanOption" in new WithPhones {
    EdmCoreUtils.parseYesNoBooleanOption("yes") should be (Some(true))
  }

  it should "return true when string Yes when parseYesNoBooleanOption" in new WithPhones {
    EdmCoreUtils.parseYesNoBooleanOption("Yes") should be (Some(true))
  }

  it should "return true when string YeS when parseYesNoBooleanOption" in new WithPhones {
    EdmCoreUtils.parseYesNoBooleanOption("YeS") should be (Some(true))
  }

  it should "return false when string N when parseYesNoBooleanOption" in new WithPhones {
    EdmCoreUtils.parseYesNoBooleanOption("N") should be (Some(false))
  }

  it should "return false when string n when parseYesNoBooleanOption" in new WithPhones {
    EdmCoreUtils.parseYesNoBooleanOption("n") should be (Some(false))
  }

  it should "return false when string NO when parseYesNoBooleanOption" in new WithPhones {
    EdmCoreUtils.parseYesNoBooleanOption("NO") should be (Some(false))
  }

  it should "return false when string no when parseYesNoBooleanOption" in new WithPhones {
    EdmCoreUtils.parseYesNoBooleanOption("no") should be (Some(false))
  }

  it should "return false when string No when parseYesNoBooleanOption" in new WithPhones {
    EdmCoreUtils.parseYesNoBooleanOption("No") should be (Some(false))
  }

  it should "return None if the format of the string to parseYesNoBooleanOption is incorrect" in new WithPhones {
    EdmCoreUtils.parseYesNoBooleanOption("Not a correct string") should be (None)
  }

  it should "return true when string Y when parseBoolean" in new WithPhones {
    EdmCoreUtils.parseYesNoBoolean("Y") should be (true)
  }

  it should "return true when string y when parseBoolean" in new WithPhones {
    EdmCoreUtils.parseYesNoBoolean("y") should be (true)
  }

  it should "return true when string YES when parseBoolean" in new WithPhones {
    EdmCoreUtils.parseYesNoBoolean("YES") should be (true)
  }

  it should "return true when string yes when parseBoolean" in new WithPhones {
    EdmCoreUtils.parseYesNoBoolean("yes") should be (true)
  }

  it should "return true when string Yes when parseBoolean" in new WithPhones {
    EdmCoreUtils.parseYesNoBoolean("Yes") should be (true)
  }

  it should "return true when string YeS when parseBoolean" in new WithPhones {
    EdmCoreUtils.parseYesNoBoolean("YeS") should be (true)
  }

  it should "return false when string N when parseBoolean" in new WithPhones {
    EdmCoreUtils.parseYesNoBoolean("N") should be (false)
  }

  it should "return false when string n when parseBoolean" in new WithPhones {
    EdmCoreUtils.parseYesNoBoolean("n") should be (false)
  }

  it should "return false when string NO when parseBoolean" in new WithPhones {
    EdmCoreUtils.parseYesNoBoolean("NO") should be (false)
  }

  it should "return false when string no when parseBoolean" in new WithPhones {
    EdmCoreUtils.parseYesNoBoolean("no") should be (false)
  }

  it should "return false when string No when parseBoolean" in new WithPhones {
    EdmCoreUtils.parseYesNoBoolean("No") should be (false)
  }

  it should "throw an exception if the format of the string to parse as boolean is incorrect" in new WithPhones {
    an [MatchError] should be thrownBy EdmCoreUtils.parseYesNoBoolean("Not a correct string")
  }

  it should "return string when string is not $null$" in new WithPhones {
    EdmCoreUtils.parseNullString("no") should be ("no")
  }

  it should "return empty string when string is $null$" in new WithPhones {
    EdmCoreUtils.parseNullString("$null$") should be ("")
  }

  it should "return empty string when string is $_$" in new WithPhones {
    EdmCoreUtils.parseNullString("_") should be ("")
  }

  it should "convert not null string to Some" in new WithPhones {
    EdmCoreUtils.parseString("A") should be (Some("A"))
  }

  it should "convert $_$ or null string to None" in {
    EdmCoreUtils.parseString("_") should be (None)
    EdmCoreUtils.parseString("") should be (None)
    EdmCoreUtils.parseString("$null$") should be (None)
  }

  it should "only accept days from 1 to 7 when converting to saudi day of week (starting on Sunday)" in {
    an[Exception] should be thrownBy(EdmCoreUtils.saudiDayOfWeek(9))
  }

  it should "get the saudi day of week (starting on Sunday)" in {
    EdmCoreUtils.saudiDayOfWeek(4) should be (5)
  }

  it should "get the saudi day of week (starting on Sunday) when Joda day is Sunday" in {
    EdmCoreUtils.saudiDayOfWeek(7) should be (1)
  }

  it should "get the proper saudi week for the first Saturday of the year" in new WithDates {
    EdmCoreUtils.saudiWeekOfYear(firstSaturdayOfTheYear) should be (1)
  }

  it should "get the proper saudi week for the first Sunday of the year" in new WithDates {
    EdmCoreUtils.saudiWeekOfYear(firstSundayOfTheYear) should be (2)
  }
}
