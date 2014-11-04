/*
 * TODO: License goes here!
 */

package sa.com.mobily.event

case class Event(
    beginTime: Long,
    ci: String,
    eci: String,
    endTime: Long,
    eventType: Short,
    imei: Long,
    imsi: Long,
    ixc: Option[String],
    lac: String,
    mcc: Option[String],
    mnc: Option[String],
    msisdn: Long,
    rac: String,
    rat: Short,
    sac: String,
    tac: String,
    inSpeed: Option[Double] = None,
    outSpeed: Option[Double] = None)
