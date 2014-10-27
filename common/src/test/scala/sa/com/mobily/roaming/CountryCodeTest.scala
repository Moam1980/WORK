/*
 * TODO: License goes here!
 */

package sa.com.mobily.roaming

import org.scalatest.{FlatSpec, ShouldMatchers}

class CountryCodeTest extends FlatSpec with ShouldMatchers {

  trait WithCountryCode {

    val saudiArabiaMcc = "420"
    val mobilyMnc = "03"
    val saudiArabiaCallingCode = "966"
    val saudiArabiaCountry = Country("420", "966", "SA", "Saudi Arabia")
    val ksaCountryOperatorsNumber = 4
    val mobilyCountryOperator = CountryOperator("03", "Etihad/Etisalat/Mobily", saudiArabiaCountry)
  }

  "MccCountryLookup" should "have country data for a specific MCC" in new WithCountryCode {
    CountryCode.MccCountryLookup(saudiArabiaMcc) should be (saudiArabiaCountry)
  }

  "MccMncCountryLookup" should "have country data for a specific pair of MCC and MNC" in new WithCountryCode {
    CountryCode.MccMncCountryLookup(saudiArabiaMcc + mobilyMnc) should be (saudiArabiaCountry)
  }

  "MccCountryOperatorsLookup" should "have country operator data for a specific MCC" in new WithCountryCode {
    val ksaCountryOperators = CountryCode.MccCountryOperatorsLookup(saudiArabiaMcc)
    ksaCountryOperators.size should be (ksaCountryOperatorsNumber)
    ksaCountryOperators should contain (mobilyCountryOperator)
  }

  "CallingCodeCountryLookup" should "have country data for a specific calling code" in new WithCountryCode {
    CountryCode.CallingCodeCountryLookup(saudiArabiaCallingCode) should be (saudiArabiaCountry)
  }
}
