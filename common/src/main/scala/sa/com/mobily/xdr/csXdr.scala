/*
 * TODO: License goes here!
 */

package sa.com.mobily.xdr

case class CsUser(
    imei: Option[String],
    imsi: Option[String],
    msisdn: Option[String],
    imeisv: Option[String],
    tmsi: Option[String],
    oldTmsi: Option[String])

case class CsCell(
    firstLac: Option[String],
    secondLac: Option[String],
    thirdLac: Option[String],
    oldLac: Option[String],
    newLac: Option[String])

case class CsCall(callType: Option[Short], calledSsn: Option[Short])

case class CsConnection(majorMinor: Short, transactionId: Option[String])

case class CsTime(
    begin: String,
    end: String,
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
