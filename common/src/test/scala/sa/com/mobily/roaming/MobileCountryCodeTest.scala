/*
 * TODO: License goes here!
 */

package sa.com.mobily.roaming

import org.scalatest.{ShouldMatchers, FlatSpec}

class MobileCountryCodeTest extends FlatSpec with ShouldMatchers {

  trait WithMobileCountryCodes {

    val saudiArabiaCode = "420"
    val saudiArabiaCountry = Country("SA", "Saudi Arabia")
    val spainCode = "214"
    val spainCountry = Country("ES", "Spain")
  }

  "MobileCountryCode" should "have country data for a specific MCC" in new WithMobileCountryCodes {
    MobileCountryCode.MobileCountryCodeLookup(saudiArabiaCode) should be (saudiArabiaCountry)
    MobileCountryCode.MobileCountryCodeLookup(spainCode) should be (spainCountry)
  }

  it should "contain all countries" in {
    MobileCountryCode.MobileCountryCodeLookup.size should be (214)
  }
}
