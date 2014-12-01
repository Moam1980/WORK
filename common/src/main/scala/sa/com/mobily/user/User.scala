/*
 * TODO: License goes here!
 */

package sa.com.mobily.user

import sa.com.mobily.metrics.MeasurableById
import sa.com.mobily.roaming.CountryCode

case class User(
    imei: String,
    imsi: String,
    msisdn: Long) extends MeasurableById[Long] {

  lazy val mcc: String = imsi.substring(User.MccStartIndex, User.MncStartIndex)
  lazy val mnc: String = {
    val mncs = CountryCode.MccCountryOperatorsLookup(mcc).map(_.mnc)
    mncs.find(mnc => imsi.substring(User.MncStartIndex).startsWith(mnc)).getOrElse(User.UnknownMnc)
  }

  override def id: Long = msisdn
}

object User {

  val MccStartIndex = 0
  val MncStartIndex = 3
  val UnknownMnc = "Unknown"

  def header: Array[String] = {
    Array[String]("imei", "imsi", "msisdn", "mcc", "mnc")
  }

  def fields(user: User): Array[String] = {
    Array[String](
      user.imei,
      user.imsi,
      user.msisdn.toString,
      user.mcc,
      user.mnc)
  }
}
