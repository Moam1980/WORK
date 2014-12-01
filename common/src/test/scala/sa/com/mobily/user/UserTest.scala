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
    User.fields(user) should be (userFields)
  }

  it should "generate header" in new WithUser {
    User.header should be (header)
  }
}
