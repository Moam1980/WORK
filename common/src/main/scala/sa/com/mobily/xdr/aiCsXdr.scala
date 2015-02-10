/*
 * TODO: License goes here!
 */

package sa.com.mobily.xdr

import scala.language.{existentials, implicitConversions}
import scala.util.Try

import org.apache.spark.sql._

import sa.com.mobily.event.{CsAInterfaceSource, Event}
import sa.com.mobily.parsing.{CsvParser, OpenCsvParser, RowParser}
import sa.com.mobily.user.User
import sa.com.mobily.utils.EdmCoreUtils

final case class  AiCall(
    csCall: CsCall,
    scenario: String,
    calledNo: Option[String],
    calledNai: Option[String],
    callingNumber: Option[Long],
    callingNai: Option[String],
    speechDividedDataIndicator: Option[String],
    assignedChannelType: Option[String],
    speechVersionDividedDataRate: Option[String],
    noOfHandoverPerformed: Option[String],
    firstHandoverReferenceNo: Option[String],
    secondHandoverReferenceNo: Option[String],
    isInterSystemHandoverOccurred: Option[String],
    msPowerClass: Option[String],
    pagingResponse: Option[String],
    pcmMultiplexNumber: Option[String],
    frequencyBandUsed: Option[String],
    imsiDetachStatus: Option[String])

final case class  AiConnection(
    csConnection: CsConnection,
    servingRncId: Option[String])

final case class  AiCell(
    csCell: CsCell,
    cic: Option[String],
    firstCellId: Option[String],
    secondCellId: Option[String],
    thirdCellId: Option[String],
    servingLac: Option[String],
    servingCellId: Option[String],
    oldMcc: Option[String],
    oldMnc: Option[String]) {

  lazy val id: (Option[Int], Option[Int]) =
    if (csCell.firstLac.isDefined && !csCell.firstLac.get.isEmpty)
      if (firstCellId.isDefined && !firstCellId.get.isEmpty)
        (EdmCoreUtils.hexToDecimal(csCell.firstLac.get), EdmCoreUtils.hexToDecimal(firstCellId.get))
      else (EdmCoreUtils.hexToDecimal(csCell.firstLac.get), None)
    else
    if (firstCellId.isDefined && !firstCellId.get.isEmpty) (None, Some(EdmCoreUtils.hexToInt(firstCellId.get)))
    else (None, None)

  def validId: Boolean = id._1.isDefined && id._2.isDefined

  def fields: Array[String] =
    csCell.fields ++ Array(cic.getOrElse(""), firstCellId.getOrElse(""), secondCellId.getOrElse(""),
      thirdCellId.getOrElse(""), servingLac.getOrElse(""), servingCellId.getOrElse(""), oldMcc.getOrElse(""),
      oldMnc.getOrElse(""))

  def idFields: Array[String] = Array(id._1.getOrElse("").toString, id._2.getOrElse("").toString)

  override def equals(other: Any): Boolean = other match {
    case that: AiCell => (that canEqual this) && (this.id == that.id)
    case _ => false
  }

  override def hashCode: Int = id.hashCode

  def canEqual(other: Any): Boolean = other.isInstanceOf[AiCell]
}

object AiCell {

  def header: Array[String] =
    CsCell.header ++ Array("cic", "firstCellId", "secondCellId", "thirdCellId", "servingLac", "servingCellId") ++
      Array( "oldMcc", "oldMnc")

  def idHeader: Array[String] = Array("firstLac", "firstCellId")
}

final case class  AiTime(
    csTime: CsTime,
    setupTime: Option[String],
    ringingTime: Option[String],
    completeTimeDividedSmsFirstDeliveryTime: Option[String],
    assignmentTime: Option[String],
    tchAssignmentTime: Option[String],
    pagingTime: Option[String],
    callSetupTime: Option[String],
    pcmActiveTimeSlot: Option[String])

final case class  AiError(
    rrCauseBssmap: Option[String],
    causeBssmap: Option[String],
    lcsCauseBssmap: Option[String],
    returnErrorCauseBssmap: Option[String],
    rejectCauseDtapmm: Option[String],
    causeDtapcc: Option[String],
    hoRequiredCause: Option[String],
    hoFailureCause: Option[String],
    assignedCipherModeRejectCause: Option[String],
    noHandoverFailure: Option[String],
    ssErrorCode: Option[String])

final case class  AiCause(
    csCause: CsCause,
    endMessage: Option[String],
    sccpRelease: Option[Short],
    hoRequest: Option[String],
    hoRequiredReject: Option[String],
    assignmentFailure: Option[String],
    authenticationFailure: Option[String],
    serviceCategory: Option[String],
    sccpConnectionRefusal: Option[Short],
    causeLocation: Option[String])

final case class  AiFlag(
    csFlag: CsFlag,
    bssmapMessage: Option[String],
    ssMessage: Option[String],
    sdcchDrop: Option[String])

final case class  AiChannel(
    causeDtaprr: Option[String],
    firstChannelType: Option[String],
    secondChannelType: Option[String],
    firstRadioTimeSlot: Option[String],
    secondRadioTimeSlot: Option[String],
    firstArfcn: Option[String],
    secondArfcn: Option[String],
    srnti: Option[String],
    redirectingPartyOrServiceCentreNo: Option[String],
    redirectingPartyOrServiceCentreNoa: Option[String])

final case class  AiCsXdr(
    call: AiCall,
    cell: AiCell,
    time: AiTime,
    user: CsUser,
    pointCode: CsPointCode,
    error: AiError,
    ttype: CsType,
    cause: AiCause,
    flag: AiFlag,
    transport: CsTransport,
    channel: AiChannel,
    operation: CsOperation,
    statistic: CsStatistic,
    connection: AiConnection) {

  def toEvent: Event = {
    Event(
      User(imei= "", imsi = user.imsi.getOrElse(""), msisdn = 0L),
      beginTime = EdmCoreUtils.hexToLong(time.csTime.begin),
      endTime = EdmCoreUtils.hexToLong(time.csTime.end),
      lacTac = cell.id._1.get,
      cellId = cell.id._2.get,
      source = CsAInterfaceSource,
      eventType = EdmCoreUtils.hexToDecimal(call.scenario).map(_.toString),
      subsequentLacTac = Try { cell.csCell.secondLac.get.toInt }.toOption,
      subsequentCellId = Try { cell.secondCellId.get.toInt }.toOption)
  }
}

object AiCsXdr {

  final val lineCsvParserObject = new OpenCsvParser(separator = ',')

  implicit val fromCsv = new CsvParser[AiCsXdr] {

    override def lineCsvParser: OpenCsvParser = lineCsvParserObject

    override def fromFields(fields: Array[String]): AiCsXdr = { // scalastyle:ignore method.length
      val (chunk0, remainingChunk0) = fields.splitAt(8) // scalastyle:ignore magic.number
      val (chunk1, remainingChunk1) = remainingChunk0.splitAt(5) // scalastyle:ignore magic.number
      val (chunk2, remainingChunk2) = remainingChunk1.splitAt(7) // scalastyle:ignore magic.number
      val (chunk3, remainingChunk3) = remainingChunk2.splitAt(5) // scalastyle:ignore magic.number
      val (chunk4, remainingChunk4) = remainingChunk3.splitAt(3) // scalastyle:ignore magic.number
      val (chunk5, remainingChunk5) = remainingChunk4.splitAt(6) // scalastyle:ignore magic.number
      val (chunk6, remainingChunk6) = remainingChunk5.splitAt(5) // scalastyle:ignore magic.number
      val (chunk7, remainingChunk7) = remainingChunk6.splitAt(7) // scalastyle:ignore magic.number
      val (chunk8, remainingChunk8) = remainingChunk7.splitAt(3) // scalastyle:ignore magic.number
      val (chunk9, remainingChunk9) = remainingChunk8.splitAt(4) // scalastyle:ignore magic.number
      val (chunk10, remainingChunk10) = remainingChunk9.splitAt(6) // scalastyle:ignore magic.number
      val (chunk11, remainingChunk11) = remainingChunk10.splitAt(5) // scalastyle:ignore magic.number
      val (chunk12, remainingChunk12) = remainingChunk11.splitAt(3) // scalastyle:ignore magic.number
      val (chunk13, remainingChunk13) = remainingChunk12.splitAt(4) // scalastyle:ignore magic.number
      val (chunk14, remainingChunk14) = remainingChunk13.splitAt(4) // scalastyle:ignore magic.number
      val (chunk15, remainingChunk15) = remainingChunk14.splitAt(5) // scalastyle:ignore magic.number
      val (chunk16, remainingChunk16) = remainingChunk15.splitAt(4) // scalastyle:ignore magic.number
      val (chunk17, remainingChunk17) = remainingChunk16.splitAt(3) // scalastyle:ignore magic.number
      val (chunk18, remainingChunk18) = remainingChunk17.splitAt(3) // scalastyle:ignore magic.number
      val (chunk19, remainingChunk19) = remainingChunk18.splitAt(5) // scalastyle:ignore magic.number
      val (chunk20, remainingChunk20) = remainingChunk19.splitAt(5) // scalastyle:ignore magic.number
      val (chunk21, remainingChunk21) = remainingChunk20.splitAt(6) // scalastyle:ignore magic.number
      val (chunk22, chunk23) = remainingChunk21.splitAt(5) // scalastyle:ignore magic.number

      val Array(scenario, begin, end, origin, destination, callType, calledNo, calledNai) = chunk0
      val Array(callingNumber, callingNai, cic, speechDividedDataIndicator, assignedChannelType) = chunk1
      val Array(speechVersionDividedDataRate, imei, imsi, tmsi, firstLac, firstCellId, secondLac) = chunk2
      val Array(secondCellId, setupTime, holding, ringingTime, conversation) = chunk3
      val Array(completeTimeDividedSmsFirstDeliveryTime, assignmentTime, tchAssignmentTime) = chunk4
      val Array(pagingTime, smsLength, noOfHandoverPerformed, rrCauseBssmap, causeBssmap, lcsCauseBssmap) = chunk5
      val Array(returnErrorCauseBssmap, rejectCauseDtapmm, causeDtapcc, rpCauseDtapSms, cpCauseDtapSms) = chunk6
      val Array(thirdLac, thirdCellId, hoRequiredCause, hoFailureCause, oldMcc, oldMnc, oldLac) = chunk7
      val Array(assignedCipherModeRejectCause, noHandoverFailure, disconnection) = chunk8
      val Array(firstHandoverReferenceNo, secondHandoverReferenceNo, transactionId, imeisv) = chunk9
      val Array(oldTmsi, imsiDetachStatus, locationUpdateType, causeLocation, endMessage, endMessageCause) = chunk10
      val Array(sequenceTerminateCause, sccpReleaseCause, calledSsn, hoCompleteTime, hoRequestCause) = chunk11
      val Array(hoRequiredRejectCause, assignmentFailureCause, cmServiceRequestType) = chunk12
      val Array(authenticationFailureCause, serviceCategory, callOnHold, dtmfRejectCause) = chunk13
      val Array(dtmfNumberBits, holdRetrieveRejectCause, referenceNo, protocolIdentifier) = chunk14
      val Array(commandType, status, failureCause, causeDtaprr, firstChannelType) = chunk15
      val Array(secondChannelType, firstRadioTimeSlot, secondRadioTimeSlot, firstArfcn) = chunk16
      val Array(secondArfcn, srnti, redirectingPartyOrServiceCentreNo) = chunk17
      val Array(redirectingPartyOrServiceCentreNoa, sccpConnectionRefusalCause, bssmapMessage) = chunk18
      val Array(mmMessage, ccMessage, smsMessage, ssMessage, ccCode1) = chunk19
      val Array(ccCode2, ssCode1, ssCode2, ssErrorCode, majorMinor) = chunk20
      val Array(newLac, servingLac, servingCellId, servingRncId, callSetupTime, complete) = chunk21
      val Array(msPowerClass, pagingResponse, pcmMultiplexNumber, pcmActiveTimeSlot, msisdn) = chunk22
      val Array(isInterSystemHandoverOccurred, sdcchDrop, frequencyBandUsed) = chunk23

      AiCsXdr(
        call = AiCall(
          csCall = CsCall(callType = EdmCoreUtils.parseShort(callType), calledSsn = EdmCoreUtils.parseShort(calledSsn)),
          scenario = EdmCoreUtils.parseNullString(scenario),
          calledNo = EdmCoreUtils.parseString(calledNo),
          calledNai = EdmCoreUtils.parseString(calledNai),
          callingNumber = EdmCoreUtils.longOption(callingNumber),
          callingNai = EdmCoreUtils.parseString(callingNai),
          speechDividedDataIndicator = EdmCoreUtils.parseString(speechDividedDataIndicator),
          assignedChannelType = EdmCoreUtils.parseString(assignedChannelType),
          speechVersionDividedDataRate = EdmCoreUtils.parseString(speechVersionDividedDataRate),
          noOfHandoverPerformed = EdmCoreUtils.parseString(noOfHandoverPerformed),
          firstHandoverReferenceNo = EdmCoreUtils.parseString(firstHandoverReferenceNo),
          secondHandoverReferenceNo = EdmCoreUtils.parseString(secondHandoverReferenceNo),
          isInterSystemHandoverOccurred = EdmCoreUtils.parseString(isInterSystemHandoverOccurred),
          msPowerClass = EdmCoreUtils.parseString(msPowerClass),
          pagingResponse = EdmCoreUtils.parseString(pagingResponse),
          pcmMultiplexNumber = EdmCoreUtils.parseString(pcmMultiplexNumber),
          frequencyBandUsed = EdmCoreUtils.parseString(frequencyBandUsed),
          imsiDetachStatus = EdmCoreUtils.parseString(imsiDetachStatus)),
        cell = AiCell(
          csCell = CsCell(
            firstLac = EdmCoreUtils.parseString(firstLac),
            secondLac = EdmCoreUtils.parseString(secondLac),
            thirdLac = EdmCoreUtils.parseString(thirdLac),
            oldLac = EdmCoreUtils.parseString(oldLac),
            newLac = EdmCoreUtils.parseString(newLac)),
          cic = EdmCoreUtils.parseString(cic),
          firstCellId = EdmCoreUtils.parseString(firstCellId),
          secondCellId = EdmCoreUtils.parseString(secondCellId),
          thirdCellId = EdmCoreUtils.parseString(thirdCellId),
          servingLac = EdmCoreUtils.parseString(servingLac),
          servingCellId = EdmCoreUtils.parseString(servingCellId),
          oldMcc = EdmCoreUtils.parseString(oldMcc),
          oldMnc = EdmCoreUtils.parseString(oldMnc)),
        time = AiTime(
          csTime = CsTime(
            begin = EdmCoreUtils.parseNullString(begin),
            end = EdmCoreUtils.parseNullString(end),
            complete = EdmCoreUtils.parseString(complete),
            callOnHold = EdmCoreUtils.parseString(callOnHold),
            holding = EdmCoreUtils.parseString(holding),
            conversation = EdmCoreUtils.parseString(conversation)),
          setupTime = EdmCoreUtils.parseString(setupTime),
          ringingTime = EdmCoreUtils.parseString(ringingTime),
          completeTimeDividedSmsFirstDeliveryTime = EdmCoreUtils.parseString(completeTimeDividedSmsFirstDeliveryTime),
          assignmentTime = EdmCoreUtils.parseString(assignmentTime),
          tchAssignmentTime = EdmCoreUtils.parseString(tchAssignmentTime),
          pagingTime = EdmCoreUtils.parseString(pagingTime),
          callSetupTime = EdmCoreUtils.parseString(callSetupTime),
          pcmActiveTimeSlot = EdmCoreUtils.parseString(pcmActiveTimeSlot)),
        user = CsUser(
          imei = EdmCoreUtils.parseString(imei),
          imsi = EdmCoreUtils.parseString(imsi),
          msisdn = EdmCoreUtils.validMsisdn(msisdn),
          imeisv = EdmCoreUtils.parseString(imeisv),
          tmsi = EdmCoreUtils.parseString(tmsi),
          oldTmsi = EdmCoreUtils.parseString(oldTmsi)),
        pointCode = CsPointCode(
          origin = EdmCoreUtils.parseNullString(origin),
          destination = EdmCoreUtils.parseNullString(destination)),
        error = AiError(
          rrCauseBssmap = EdmCoreUtils.parseString(rrCauseBssmap),
          causeBssmap = EdmCoreUtils.parseString(causeBssmap),
          lcsCauseBssmap = EdmCoreUtils.parseString(lcsCauseBssmap),
          returnErrorCauseBssmap = EdmCoreUtils.parseString(returnErrorCauseBssmap),
          rejectCauseDtapmm = EdmCoreUtils.parseString(rejectCauseDtapmm),
          causeDtapcc = EdmCoreUtils.parseString(causeDtapcc),
          hoRequiredCause = EdmCoreUtils.parseString(hoRequiredCause),
          hoFailureCause = EdmCoreUtils.parseString(hoFailureCause),
          assignedCipherModeRejectCause = EdmCoreUtils.parseString(assignedCipherModeRejectCause),
          noHandoverFailure = EdmCoreUtils.parseString(noHandoverFailure),
          ssErrorCode = EdmCoreUtils.parseString(ssErrorCode)),
        ttype = CsType(
          locationUpdate = EdmCoreUtils.hexToShort(locationUpdateType),
          cmServiceRequest = EdmCoreUtils.hexToShort(cmServiceRequestType)),
        cause = AiCause(
          csCause = CsCause(
            disconnection = EdmCoreUtils.hexToShort(disconnection),
            dtmfReject = EdmCoreUtils.hexToShort(dtmfRejectCause),
            holdRetrieveReject = EdmCoreUtils.hexToShort(holdRetrieveRejectCause),
            rp = EdmCoreUtils.hexToShort(rpCauseDtapSms),
            cp = EdmCoreUtils.hexToShort(cpCauseDtapSms),
            sequenceTerminate = EdmCoreUtils.hexToShort(sequenceTerminateCause)),
          endMessage = EdmCoreUtils.parseString(endMessageCause),
          sccpRelease = EdmCoreUtils.parseShort(sccpReleaseCause),
          hoRequest = EdmCoreUtils.parseString(hoRequestCause),
          hoRequiredReject = EdmCoreUtils.parseString(hoRequiredRejectCause),
          assignmentFailure = EdmCoreUtils.parseString(assignmentFailureCause),
          authenticationFailure = EdmCoreUtils.parseString(authenticationFailureCause),
          serviceCategory = EdmCoreUtils.parseString(serviceCategory),
          sccpConnectionRefusal = EdmCoreUtils.parseShort(sccpConnectionRefusalCause),
          causeLocation = EdmCoreUtils.parseString(causeLocation)),
        flag = AiFlag(
          csFlag = CsFlag(
            ccMessage = EdmCoreUtils.parseString(ccMessage),
            mmMessage = EdmCoreUtils.parseString(mmMessage),
            smsMessage = EdmCoreUtils.parseString(smsMessage)),
          bssmapMessage = EdmCoreUtils.parseString(bssmapMessage),
          ssMessage = EdmCoreUtils.parseString(ssMessage),
          sdcchDrop = EdmCoreUtils.parseString(sdcchDrop)),
        transport = CsTransport(
          referenceNo = EdmCoreUtils.parseString(referenceNo),
          protocolIdentifier = EdmCoreUtils.hexToShort(protocolIdentifier),
          commandType = EdmCoreUtils.parseShort(commandType),
          status = EdmCoreUtils.parseShort(status),
          failureCause = EdmCoreUtils.hexToShort(failureCause)),
        channel = AiChannel(
          causeDtaprr = EdmCoreUtils.parseString(causeDtaprr),
          firstChannelType = EdmCoreUtils.parseString(firstChannelType),
          secondChannelType = EdmCoreUtils.parseString(secondChannelType),
          firstRadioTimeSlot = EdmCoreUtils.parseString(firstRadioTimeSlot),
          secondRadioTimeSlot = EdmCoreUtils.parseString(secondRadioTimeSlot),
          firstArfcn = EdmCoreUtils.parseString(firstArfcn),
          secondArfcn = EdmCoreUtils.parseString(secondArfcn),
          srnti = EdmCoreUtils.parseString(srnti),
          redirectingPartyOrServiceCentreNo = EdmCoreUtils.parseString(redirectingPartyOrServiceCentreNo),
          redirectingPartyOrServiceCentreNoa = EdmCoreUtils.parseString(redirectingPartyOrServiceCentreNoa)),
        operation = CsOperation(
          ccCode1 = EdmCoreUtils.parseInt(ccCode1),
          ccCode2 = EdmCoreUtils.parseInt(ccCode2),
          ssCode1 = EdmCoreUtils.parseInt(ssCode1),
          ssCode2 = EdmCoreUtils.parseInt(ssCode2)),
        statistic = CsStatistic(
            smsLength = EdmCoreUtils.parseString(smsLength),
            dtmfNumberBits = EdmCoreUtils.parseString(dtmfNumberBits)),
        connection = AiConnection(
          csConnection = CsConnection(
            majorMinor = majorMinor.toShort,
            transactionId = EdmCoreUtils.parseString(transactionId)),
          servingRncId = EdmCoreUtils.parseString(servingRncId)))
    }
  }

  implicit val fromRow = new RowParser[AiCsXdr] {

    override def fromRow(row: Row): AiCsXdr = { // scalastyle:ignore method.length
      val Seq(
        callRow,
        cellRow,
        timeRow,
        userRow,
        pointRow,
        errorRow,
        typeRow,
        causeRow,
        flagRow,
        transportRow,
        channelRow,
        operationRow,
        statisticRow,
        connectionRow) = row.asInstanceOf[Seq[Row]]
      val Seq(
        Seq(callType, calledSsn),
        scenario,
        calledNo,
        calledNai,
        callingNumber,
        callingNai,
        speechDividedDataIndicator,
        assignedChannelType,
        speechVersionDividedDataRate,
        noOfHandoverPerformed,
        firstHandoverReferenceNo,
        secondHandoverReferenceNo,
        isInterSystemHandoverOccurred,
        msPowerClass,
        pagingResponse,
        pcmMultiplexNumber,
        frequencyBandUsed,
        imsiDetachStatus) = callRow
      val Seq(
        Seq(firstLac, secondLac, thirdLac, oldLac, newLac),
        cic,
        firstCellId,
        secondCellId,
        thirdCellId,
        servingLac,
        servingCellId,
        oldMcc,
        oldMnc) = cellRow
      val Seq(
        Seq(begin, end, complete, callOnHold, holding, conversation),
        setupTime,
        ringingTime,
        completeTimeDividedSmsFirstDeliveryTime,
        assignmentTime,
        tchAssignmentTime,
        pagingTime,
        callSetupTime,
        pcmActiveTimeSlot) = timeRow
      val Seq(
        imei,
        imsi,
        msisdn,
        imeisv,
        tmsi,
        oldTmsi) = userRow
      val Seq(origin, destination) = pointRow
      val Seq(
        rrCauseBssmap,
        causeBssmap,
        lcsCauseBssmap,
        returnErrorCauseBssmap,
        rejectCauseDtapmm,
        causeDtapcc,
        hoRequiredCause,
        hoFailureCause,
        assignedCipherModeRejectCause,
        noHandoverFailure,
        ssErrorCode) = errorRow
      val Seq(locationUpdate, cmServiceRequest) = typeRow
      val Seq(
        Seq(disconnection, dtmfRejectCause, holdRetrieveReject, rp, cp, sequenceTerminate),
        endMessage,
        sccpRelease,
        hoRequest,
        hoRequiredReject,
        assignmentFailure,
        authenticationFailure,
        serviceCategory,
        sccpConnectionRefusal,
        causeLocation) = causeRow
      val Seq(
        Seq(ccMessage, mmMessage, smsMessage),
        bssmapMessage,
        ssMessage,
        sdcchDrop) = flagRow
      val Seq(
        referenceNo,
        protocolIdentifier,
        commandType,
        status,
        failureCause) = transportRow
      val Seq(
        causeDtaprr,
        firstChannelType,
        secondChannelType,
        firstRadioTimeSlot,
        secondRadioTimeSlot,
        firstArfcn,
        secondArfcn,
        srnti,
        redirectingPartyOrServiceCentreNo,
        redirectingPartyOrServiceCentreNoa) = channelRow
      val Seq(ccCode1, ccCode2, ssCode1, ssCode2) = operationRow
      val Seq(smsLength, dtmfNumberBits) = statisticRow
      val Seq(Seq(majorMinor, transactionId), servingRncId) = connectionRow

      AiCsXdr(
        call = AiCall(
          csCall = CsCall(
            callType = EdmCoreUtils.shortOption(callType),
            calledSsn = EdmCoreUtils.shortOption(calledSsn)),
          scenario = scenario.asInstanceOf[String],
          calledNo = EdmCoreUtils.stringOption(calledNo),
          calledNai = EdmCoreUtils.stringOption(calledNai),
          callingNumber = EdmCoreUtils.longOption(callingNumber),
          callingNai = EdmCoreUtils.stringOption(callingNai),
          speechDividedDataIndicator = EdmCoreUtils.stringOption(speechDividedDataIndicator),
          assignedChannelType = EdmCoreUtils.stringOption(assignedChannelType),
          speechVersionDividedDataRate = EdmCoreUtils.stringOption(speechVersionDividedDataRate),
          noOfHandoverPerformed = EdmCoreUtils.stringOption(noOfHandoverPerformed),
          firstHandoverReferenceNo = EdmCoreUtils.stringOption(firstHandoverReferenceNo),
          secondHandoverReferenceNo = EdmCoreUtils.stringOption(secondHandoverReferenceNo),
          isInterSystemHandoverOccurred = EdmCoreUtils.stringOption(isInterSystemHandoverOccurred),
          msPowerClass = EdmCoreUtils.stringOption(msPowerClass),
          pagingResponse = EdmCoreUtils.stringOption(pagingResponse),
          pcmMultiplexNumber = EdmCoreUtils.stringOption(pcmMultiplexNumber),
          frequencyBandUsed = EdmCoreUtils.stringOption(frequencyBandUsed),
          imsiDetachStatus = EdmCoreUtils.stringOption(imsiDetachStatus)),
        cell = AiCell(
          csCell = CsCell(
            firstLac = EdmCoreUtils.stringOption(firstLac),
            secondLac = EdmCoreUtils.stringOption(secondLac),
            thirdLac = EdmCoreUtils.stringOption(thirdLac),
            oldLac = EdmCoreUtils.stringOption(oldLac),
            newLac = EdmCoreUtils.stringOption(newLac)),
          cic = EdmCoreUtils.stringOption(cic),
          firstCellId = EdmCoreUtils.stringOption(firstCellId),
          secondCellId = EdmCoreUtils.stringOption(secondCellId),
          thirdCellId = EdmCoreUtils.stringOption(thirdCellId),
          servingLac = EdmCoreUtils.stringOption(servingLac),
          servingCellId = EdmCoreUtils.stringOption(servingCellId),
          oldMcc = EdmCoreUtils.stringOption(oldMcc),
          oldMnc = EdmCoreUtils.stringOption(oldMnc)),
        time = AiTime(
          csTime = CsTime(
            begin = begin.asInstanceOf[String],
            end = end.asInstanceOf[String],
            complete = EdmCoreUtils.stringOption(complete),
            callOnHold = EdmCoreUtils.stringOption(callOnHold),
            holding = EdmCoreUtils.stringOption(holding),
            conversation = EdmCoreUtils.stringOption(conversation)),
          setupTime = EdmCoreUtils.stringOption(setupTime),
          ringingTime = EdmCoreUtils.stringOption(ringingTime),
          completeTimeDividedSmsFirstDeliveryTime = EdmCoreUtils.stringOption(completeTimeDividedSmsFirstDeliveryTime),
          assignmentTime = EdmCoreUtils.stringOption(assignmentTime),
          tchAssignmentTime = EdmCoreUtils.stringOption(tchAssignmentTime),
          pagingTime = EdmCoreUtils.stringOption(pagingTime),
          callSetupTime = EdmCoreUtils.stringOption(callSetupTime),
          pcmActiveTimeSlot = EdmCoreUtils.stringOption(pcmActiveTimeSlot)),
        user = CsUser(
          imei = EdmCoreUtils.stringOption(imei),
          imsi = EdmCoreUtils.stringOption(imsi),
          msisdn = EdmCoreUtils.longOption(msisdn),
          imeisv = EdmCoreUtils.stringOption(imeisv),
          tmsi = EdmCoreUtils.stringOption(tmsi),
          oldTmsi = EdmCoreUtils.stringOption(oldTmsi)),
        pointCode = CsPointCode(
          origin = origin.asInstanceOf[String],
          destination = destination.asInstanceOf[String]),
        error = AiError(
          rrCauseBssmap = EdmCoreUtils.stringOption(rrCauseBssmap),
          causeBssmap = EdmCoreUtils.stringOption(causeBssmap),
          lcsCauseBssmap = EdmCoreUtils.stringOption(lcsCauseBssmap),
          returnErrorCauseBssmap = EdmCoreUtils.stringOption(returnErrorCauseBssmap),
          rejectCauseDtapmm = EdmCoreUtils.stringOption(rejectCauseDtapmm),
          causeDtapcc = EdmCoreUtils.stringOption(causeDtapcc),
          hoRequiredCause = EdmCoreUtils.stringOption(hoRequiredCause),
          hoFailureCause = EdmCoreUtils.stringOption(hoFailureCause),
          assignedCipherModeRejectCause = EdmCoreUtils.stringOption(assignedCipherModeRejectCause),
          noHandoverFailure = EdmCoreUtils.stringOption(noHandoverFailure),
          ssErrorCode = EdmCoreUtils.stringOption(ssErrorCode)),
        ttype = CsType(
          locationUpdate = EdmCoreUtils.shortOption(locationUpdate),
          cmServiceRequest = EdmCoreUtils.shortOption(cmServiceRequest)),
        cause = AiCause(
          CsCause(
            disconnection = EdmCoreUtils.shortOption(disconnection),
            dtmfReject = EdmCoreUtils.shortOption(dtmfRejectCause),
            holdRetrieveReject = EdmCoreUtils.shortOption(holdRetrieveReject),
            rp = EdmCoreUtils.shortOption(rp),
            cp = EdmCoreUtils.shortOption(cp),
            sequenceTerminate = EdmCoreUtils.shortOption(sequenceTerminate)),
          endMessage = EdmCoreUtils.stringOption(endMessage),
          sccpRelease = EdmCoreUtils.shortOption(sccpRelease),
          hoRequest = EdmCoreUtils.stringOption(hoRequest),
          hoRequiredReject = EdmCoreUtils.stringOption(hoRequiredReject),
          assignmentFailure = EdmCoreUtils.stringOption(assignmentFailure),
          authenticationFailure = EdmCoreUtils.stringOption(authenticationFailure),
          serviceCategory = EdmCoreUtils.stringOption(serviceCategory),
          sccpConnectionRefusal = EdmCoreUtils.shortOption(sccpConnectionRefusal),
          causeLocation = EdmCoreUtils.stringOption(causeLocation)),
        flag = AiFlag(
          csFlag = CsFlag(
            ccMessage = EdmCoreUtils.stringOption(ccMessage),
            mmMessage = EdmCoreUtils.stringOption(mmMessage),
            smsMessage = EdmCoreUtils.stringOption(smsMessage)),
          bssmapMessage = EdmCoreUtils.stringOption(bssmapMessage),
          ssMessage = EdmCoreUtils.stringOption(ssMessage),
          sdcchDrop = EdmCoreUtils.stringOption(sdcchDrop)),
        transport = CsTransport(
          referenceNo = EdmCoreUtils.stringOption(referenceNo),
          protocolIdentifier = EdmCoreUtils.shortOption(protocolIdentifier),
          commandType = EdmCoreUtils.shortOption(commandType),
          status = EdmCoreUtils.shortOption(status),
          failureCause = EdmCoreUtils.shortOption(failureCause)),
        channel = AiChannel(
          causeDtaprr = EdmCoreUtils.stringOption(causeDtaprr),
          firstChannelType = EdmCoreUtils.stringOption(firstChannelType),
          secondChannelType = EdmCoreUtils.stringOption(secondChannelType),
          firstRadioTimeSlot = EdmCoreUtils.stringOption(firstRadioTimeSlot),
          secondRadioTimeSlot = EdmCoreUtils.stringOption(secondRadioTimeSlot),
          firstArfcn = EdmCoreUtils.stringOption(firstArfcn),
          secondArfcn = EdmCoreUtils.stringOption(secondArfcn),
          srnti = EdmCoreUtils.stringOption(srnti),
          redirectingPartyOrServiceCentreNo = EdmCoreUtils.stringOption(redirectingPartyOrServiceCentreNo),
          redirectingPartyOrServiceCentreNoa = EdmCoreUtils.stringOption(redirectingPartyOrServiceCentreNoa)),
        operation = CsOperation(
          ccCode1 = EdmCoreUtils.intOption(ccCode1),
          ccCode2 = EdmCoreUtils.intOption(ccCode2),
          ssCode1 = EdmCoreUtils.intOption(ssCode1),
          ssCode2 = EdmCoreUtils.intOption(ssCode2)),
        statistic =
          CsStatistic(
            smsLength = EdmCoreUtils.stringOption(smsLength),
            dtmfNumberBits = EdmCoreUtils.stringOption(dtmfNumberBits)),
        connection = AiConnection(
          csConnection = CsConnection(majorMinor.asInstanceOf[Short], EdmCoreUtils.stringOption(transactionId)),
          servingRncId = EdmCoreUtils.stringOption(servingRncId)))
    }
  }

  def isValidToBeParsedAsEvent(aiCs: AiCsXdr): Boolean =
    aiCs.user.imsi.isDefined && aiCs.time.csTime.validTime && aiCs.cell.validId
}
