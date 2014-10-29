/*
 * TODO: License goes here!
 */

package sa.com.mobily.roaming

import org.scalatest.{ShouldMatchers, FlatSpec}

class CountryCallingCodeTest extends FlatSpec with ShouldMatchers {

  trait WithCountryCallingCodes {

    val saudiArabiaCode = 966
    val saudiArabiaCountry = "Saudi Arabia"
    val spainCode = 34
    val spainCountry = "Spain"
  }

  "CountryCallingCode" should "have country data for a specific MCC" in new WithCountryCallingCodes {
    CountryCallingCode.CountryCallingCodeLookup(saudiArabiaCode) should be (saudiArabiaCountry)
    CountryCallingCode.CountryCallingCodeLookup(spainCode) should be (spainCountry)
  }

  it should "contain all countries" in {
    CountryCallingCode.CountryCallingCodeLookup.size should be (569)
  }

  it should "max length country code should be 6" in {
    CountryCallingCode.maxLengthCountryCallingCode should be (6)
  }
}
