/*
 * TODO: License goes here!
 */

package sa.com.mobily.xdr

import sa.com.mobily.event.Event

case class CsUser(
    imei: Option[String],
    imsi: Option[String],
    msisdn: Option[Long],
    imeisv: Option[String],
    tmsi: Option[String],
    oldTmsi: Option[String])

case class CsCell(
    firstLac: Option[String],
    secondLac: Option[String],
    thirdLac: Option[String],
    oldLac: Option[String],
    newLac: Option[String]) {

  def fields: Array[String] =
    Array(firstLac.getOrElse(""), secondLac.getOrElse(""), thirdLac.getOrElse(""), oldLac.getOrElse(""),
      newLac.getOrElse(""))
}

object CsCell {

  def header: Array[String] = Array("firstLac", "secondLac", "thirdLac", "oldLac", "newLac")
}

case class CsCall(callType: Option[Short], calledSsn: Option[Short])

case class CsConnection(majorMinor: Short, transactionId: Option[String])

case class CsTime(
    begin: Long,
    end: Long,
    complete: Option[String],
    callOnHold: Option[String],
    holding: Option[String],
    conversation: Option[String])

case class CsType(locationUpdate: Option[Short], cmServiceRequest: Option[Short])

case class CsPointCode(origin: String, destination: String)

case class CsTransport(
    protocolIdentifier: Option[Short],
    commandType: Option[Short],
    referenceNo: Option[String],
    status: Option[Short],
    failureCause: Option[Short])

case class CsOperation(ccCode1: Option[Int], ccCode2: Option[Int], ssCode1: Option[Int], ssCode2: Option[Int])

case class CsFlag(ccMessage: Option[String], mmMessage: Option[String], smsMessage: Option[String])

case class CsCause(
    disconnection: Option[Short],
    dtmfReject: Option[Short],
    holdRetrieveReject: Option[Short],
    cp: Option[Short],
    rp: Option[Short],
    sequenceTerminate: Option[Short])

case class CsStatistic(smsLength: Option[String], dtmfNumberBits: Option[String])

object CsXdr {

  def fillUserEventWithMsisdn(subscribers: Map[String, Long], event: Event): Event = {
    if (subscribers.isDefinedAt(event.user.imsi))
      event.copy(user = event.user.copy(msisdn = subscribers(event.user.imsi)))
    else
      event
  }
}
