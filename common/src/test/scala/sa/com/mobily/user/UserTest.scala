/*
 * TODO: License goes here!
 */

package sa.com.mobily.user

import org.scalatest.{FlatSpec, ShouldMatchers}

class UserTest extends FlatSpec with ShouldMatchers {

  trait WithUser {

    val user = User(
      imei = "866173010386736",
      imsi = "420034122616618",
      msisdn = 560917079L)

    val userFields = Array[String]("866173010386736", "420034122616618", "560917079", "420", "03")
    val header = Array[String]("imei", "imsi", "msisdn", "mcc", "mnc")
  }

  trait WithEqualityUsers {

    val firstUserImei = User("imei", "firstImsi", 1)
    val secondUserImei = firstUserImei.copy(imsi = "secondImsi", msisdn = 2)
    val firstUserImsi = User("firstImei", "imsi", 1)
    val secondUserImsi = firstUserImsi.copy(imei = "secondImei", msisdn = 2)
    val firstUserMsisdn = User("firstImei", "firstImsi", 1)
    val secondUserMsisdn = firstUserMsisdn.copy(imei = "secondImei", imsi = "secondImsi")
  }

  "User" should "identify MCC from IMSI" in new WithUser {
    user.mcc should be ("420")
  }

  it should "identify MNC from IMSI" in new WithUser {
    user.mnc should be ("03")
  }

  it should "return an unknown MNC when not available in the corresponding table" in new WithUser {
    user.copy(imsi = "358856122616619").mnc should be (User.UnknownMnc)
  }

  it should "generate fields" in new WithUser {
    user.fields should be (userFields)
  }

  it should "generate header" in new WithUser {
    User.header should be (header)
  }

  it should "return the same number of fields for header and fields method" in new WithUser {
    User.header.length == user.fields.length should be(true)
  }

  it should "throw an Exception when it does not receive neither msisdn nor imsi nor imei" in {
    an [Exception] should be thrownBy User("", "", 0)
  }

  it should "compare to true two users with the same imei" in new WithEqualityUsers {
    firstUserImei == secondUserImei should be(true)
    firstUserImei.equals(secondUserImei) should be(true)
  }

  it should "compare to true two users with the same imsi" in new WithEqualityUsers {
    firstUserImsi == secondUserImsi should be(true)
    firstUserImsi.equals(secondUserImsi) should be(true)
  }

  it should "compare to true two users with the same msisdn" in new WithEqualityUsers {
    firstUserMsisdn == secondUserMsisdn should be(true)
    firstUserMsisdn.equals(secondUserMsisdn) should be(true)
  }

  it should "compare to false two users with different fields" in new WithEqualityUsers {
    firstUserImei == secondUserImsi should be(false)
    firstUserImei.equals(secondUserImsi) should be(false)
  }
}
