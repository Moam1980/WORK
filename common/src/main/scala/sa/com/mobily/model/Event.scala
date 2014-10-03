/*
 * TODO: License goes here!
 */

package sa.com.mobily.model

/**
  */
case class CsEvent(
  beginTime: Long,
  ci: String,
  eci: String,
  endTime: Long,
  eventType: Short,
  imei: Long,
  imsi: Long,
  ixc: String,
  lac: String,
  mcc: String,
  mnc: String,
  msisdn: Long,
  rac: String,
  rat: Short,
  sac: String,
  tac: String)
