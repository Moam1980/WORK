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

  require(!imei.isEmpty || !imsi.isEmpty || msisdn != 0)

  lazy val mcc: String = imsi.substring(User.MccStartIndex, User.MncStartIndex)
  lazy val mnc: String = {
    val mncs = CountryCode.MccCountryOperatorsLookup(mcc).map(_.mnc)
    mncs.find(mnc => imsi.substring(User.MncStartIndex).startsWith(mnc)).getOrElse(User.UnknownMnc)
  }

  def fields: Array[String] = Array(imei, imsi, msisdn.toString, mcc, mnc)

  override def id: Long = msisdn

  override def equals(other: Any): Boolean = other match {
    case that: User =>
      (that canEqual this) && (this.imei == that.imei || this.imsi == that.imsi || this.msisdn == that.msisdn)
    case _ => false
  }

  override def hashCode: Int =
    User.PrimeNumber * (User.PrimeNumber * (User.PrimeNumber + imei.hashCode) + imsi.hashCode) + msisdn.hashCode

  def canEqual(other: Any): Boolean = other.isInstanceOf[User]
}

object User {

  private val PrimeNumber = 41

  val MccStartIndex = 0
  val MncStartIndex = 3
  val UnknownMnc = "Unknown"

  def header: Array[String] = Array("imei", "imsi", "msisdn", "mcc", "mnc")
}
