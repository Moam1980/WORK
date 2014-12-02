/*
 * TODO: License goes here!
 */

package sa.com.mobily.xdr

import scala.language.existentials

import org.apache.spark.sql._

import sa.com.mobily.parsing.{RowParser, OpenCsvParser, CsvParser}
import sa.com.mobily.utils.EdmCoreUtils._

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
    oldMnc: Option[String])

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
    aiCall: AiCall,
    aiCell: AiCell,
    aiTime: AiTime,
    csUser: CsUser,
    csPointCode: CsPointCode,
    aiError: AiError,
    csType: CsType,
    aiCause: AiCause,
    aiFlag: AiFlag,
    csTransport: CsTransport,
    aiChannel: AiChannel,
    csOperation: CsOperation,
    csStatistic: CsStatistic,
    aiConnection: AiConnection)

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
        aiCall = AiCall(
          csCall = CsCall(callType = parseShort(callType), calledSsn = parseShort(calledSsn)),
          scenario = parseNullString(scenario),
          calledNo = parseString(calledNo),
          calledNai = parseString(calledNai),
          callingNumber = longOption(callingNumber),
          callingNai = parseString(callingNai),
          speechDividedDataIndicator = parseString(speechDividedDataIndicator),
          assignedChannelType = parseString(assignedChannelType),
          speechVersionDividedDataRate = parseString(speechVersionDividedDataRate),
          noOfHandoverPerformed = parseString(noOfHandoverPerformed),
          firstHandoverReferenceNo = parseString(firstHandoverReferenceNo),
          secondHandoverReferenceNo = parseString(secondHandoverReferenceNo),
          isInterSystemHandoverOccurred = parseString(isInterSystemHandoverOccurred),
          msPowerClass = parseString(msPowerClass),
          pagingResponse = parseString(pagingResponse),
          pcmMultiplexNumber = parseString(pcmMultiplexNumber),
          frequencyBandUsed = parseString(frequencyBandUsed),
          imsiDetachStatus = parseString(imsiDetachStatus)),
        aiCell = AiCell(
          csCell = CsCell(
            firstLac = parseString(firstLac),
            secondLac = parseString(secondLac),
            thirdLac = parseString(thirdLac),
            oldLac = parseString(oldLac),
            newLac = parseString(newLac)),
          cic = parseString(cic),
          firstCellId = parseString(firstCellId),
          secondCellId = parseString(secondCellId),
          thirdCellId = parseString(thirdCellId),
          servingLac = parseString(servingLac),
          servingCellId = parseString(servingCellId),
          oldMcc = parseString(oldMcc),
          oldMnc = parseString(oldMnc)),
        aiTime = AiTime(
          csTime = CsTime(
            begin = parseNullString(begin),
            end = parseNullString(end),
            complete = parseString(complete),
            callOnHold = parseString(callOnHold),
            holding = parseString(holding),
            conversation = parseString(conversation)),
          setupTime = parseString(setupTime),
          ringingTime = parseString(ringingTime),
          completeTimeDividedSmsFirstDeliveryTime = parseString(completeTimeDividedSmsFirstDeliveryTime),
          assignmentTime = parseString(assignmentTime),
          tchAssignmentTime = parseString(tchAssignmentTime),
          pagingTime = parseString(pagingTime),
          callSetupTime = parseString(callSetupTime),
          pcmActiveTimeSlot = parseString(pcmActiveTimeSlot)),
        csUser = CsUser(
          imei = parseString(imei),
          imsi = parseString(imsi),
          msisdn = parseString(msisdn),
          imeisv = parseString(imeisv),
          tmsi = parseString(tmsi),
          oldTmsi = parseString(oldTmsi)),
        csPointCode = CsPointCode(
          origin = parseNullString(origin),
          destination = parseNullString(destination)),
        aiError = AiError(
          rrCauseBssmap = parseString(rrCauseBssmap),
          causeBssmap = parseString(causeBssmap),
          lcsCauseBssmap = parseString(lcsCauseBssmap),
          returnErrorCauseBssmap = parseString(returnErrorCauseBssmap),
          rejectCauseDtapmm = parseString(rejectCauseDtapmm),
          causeDtapcc = parseString(causeDtapcc),
          hoRequiredCause = parseString(hoRequiredCause),
          hoFailureCause = parseString(hoFailureCause),
          assignedCipherModeRejectCause = parseString(assignedCipherModeRejectCause),
          noHandoverFailure = parseString(noHandoverFailure),
          ssErrorCode = parseString(ssErrorCode)),
        csType = CsType(
          locationUpdate = hexToShort(locationUpdateType),
          cmServiceRequest = hexToShort(cmServiceRequestType)),
        aiCause = AiCause(
          csCause = CsCause(
            disconnection = hexToShort(disconnection),
            dtmfReject = hexToShort(dtmfRejectCause),
            holdRetrieveReject = hexToShort(holdRetrieveRejectCause),
            rp = hexToShort(rpCauseDtapSms),
            cp = hexToShort(cpCauseDtapSms),
            sequenceTerminate = hexToShort(sequenceTerminateCause)),
          endMessage = parseString(endMessageCause),
          sccpRelease = parseShort(sccpReleaseCause),
          hoRequest = parseString(hoRequestCause),
          hoRequiredReject = parseString(hoRequiredRejectCause),
          assignmentFailure = parseString(assignmentFailureCause),
          authenticationFailure = parseString(authenticationFailureCause),
          serviceCategory = parseString(serviceCategory),
          sccpConnectionRefusal = parseShort(sccpConnectionRefusalCause),
          causeLocation = parseString(causeLocation)),
        aiFlag = AiFlag(
          csFlag = CsFlag(
            ccMessage = parseString(ccMessage),
            mmMessage = parseString(mmMessage),
            smsMessage = parseString(smsMessage)),
          bssmapMessage = parseString(bssmapMessage),
          ssMessage = parseString(ssMessage),
          sdcchDrop = parseString(sdcchDrop)),
        csTransport = CsTransport(
          referenceNo = parseString(referenceNo),
          protocolIdentifier = hexToShort(protocolIdentifier),
          commandType = parseShort(commandType),
          status = parseShort(status),
          failureCause = hexToShort(failureCause)),
        aiChannel = AiChannel(
          causeDtaprr = parseString(causeDtaprr),
          firstChannelType = parseString(firstChannelType),
          secondChannelType = parseString(secondChannelType),
          firstRadioTimeSlot = parseString(firstRadioTimeSlot),
          secondRadioTimeSlot = parseString(secondRadioTimeSlot),
          firstArfcn = parseString(firstArfcn),
          secondArfcn = parseString(secondArfcn),
          srnti = parseString(srnti),
          redirectingPartyOrServiceCentreNo = parseString(redirectingPartyOrServiceCentreNo),
          redirectingPartyOrServiceCentreNoa = parseString(redirectingPartyOrServiceCentreNoa)),
        csOperation = CsOperation(
          ccCode1 = parseInt(ccCode1),
          ccCode2 = parseInt(ccCode2),
          ssCode1 = parseInt(ssCode1),
          ssCode2 = parseInt(ssCode2)),
        csStatistic = CsStatistic(smsLength = parseString(smsLength), dtmfNumberBits = parseString(dtmfNumberBits)),
        aiConnection = AiConnection(
          csConnection = CsConnection(
            majorMinor = majorMinor.toShort,
            transactionId = parseString(transactionId)),
          servingRncId = parseString(servingRncId)))
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
        aiCall = AiCall(
          csCall = CsCall(
            callType = shortOption(callType),
            calledSsn = shortOption(calledSsn)),
          scenario = scenario.asInstanceOf[String],
          calledNo = stringOption(calledNo),
          calledNai = stringOption(calledNai),
          callingNumber = longOption(callingNumber),
          callingNai = stringOption(callingNai),
          speechDividedDataIndicator = stringOption(speechDividedDataIndicator),
          assignedChannelType = stringOption(assignedChannelType),
          speechVersionDividedDataRate = stringOption(speechVersionDividedDataRate),
          noOfHandoverPerformed = stringOption(noOfHandoverPerformed),
          firstHandoverReferenceNo = stringOption(firstHandoverReferenceNo),
          secondHandoverReferenceNo = stringOption(secondHandoverReferenceNo),
          isInterSystemHandoverOccurred = stringOption(isInterSystemHandoverOccurred),
          msPowerClass = stringOption(msPowerClass),
          pagingResponse = stringOption(pagingResponse),
          pcmMultiplexNumber = stringOption(pcmMultiplexNumber),
          frequencyBandUsed = stringOption(frequencyBandUsed),
          imsiDetachStatus = stringOption(imsiDetachStatus)),
        aiCell = AiCell(
          csCell = CsCell(
            firstLac = stringOption(firstLac),
            secondLac = stringOption(secondLac),
            thirdLac = stringOption(thirdLac),
            oldLac = stringOption(oldLac),
            newLac = stringOption(newLac)),
          cic = stringOption(cic),
          firstCellId = stringOption(firstCellId),
          secondCellId = stringOption(secondCellId),
          thirdCellId = stringOption(thirdCellId),
          servingLac = stringOption(servingLac),
          servingCellId = stringOption(servingCellId),
          oldMcc = stringOption(oldMcc),
          oldMnc = stringOption(oldMnc)),
        aiTime = AiTime(
          csTime = CsTime(
            begin = begin.asInstanceOf[String],
            end = end.asInstanceOf[String],
            complete = stringOption(complete),
            callOnHold = stringOption(callOnHold),
            holding = stringOption(holding),
            conversation = stringOption(conversation)),
          setupTime = stringOption(setupTime),
          ringingTime = stringOption(ringingTime),
          completeTimeDividedSmsFirstDeliveryTime = stringOption(completeTimeDividedSmsFirstDeliveryTime),
          assignmentTime = stringOption(assignmentTime),
          tchAssignmentTime = stringOption(tchAssignmentTime),
          pagingTime = stringOption(pagingTime),
          callSetupTime = stringOption(callSetupTime),
          pcmActiveTimeSlot = stringOption(pcmActiveTimeSlot)),
        csUser = CsUser(
          imei = stringOption(imei),
          imsi = stringOption(imsi),
          msisdn = stringOption(msisdn),
          imeisv = stringOption(imeisv),
          tmsi = stringOption(tmsi),
          oldTmsi = stringOption(oldTmsi)),
        csPointCode = CsPointCode(
          origin = origin.asInstanceOf[String],
          destination = destination.asInstanceOf[String]),
        aiError = AiError(
          rrCauseBssmap = stringOption(rrCauseBssmap),
          causeBssmap = stringOption(causeBssmap),
          lcsCauseBssmap = stringOption(lcsCauseBssmap),
          returnErrorCauseBssmap = stringOption(returnErrorCauseBssmap),
          rejectCauseDtapmm = stringOption(rejectCauseDtapmm),
          causeDtapcc = stringOption(causeDtapcc),
          hoRequiredCause = stringOption(hoRequiredCause),
          hoFailureCause = stringOption(hoFailureCause),
          assignedCipherModeRejectCause = stringOption(assignedCipherModeRejectCause),
          noHandoverFailure = stringOption(noHandoverFailure),
          ssErrorCode = stringOption(ssErrorCode)),
        csType = CsType(
          locationUpdate = shortOption(locationUpdate),
          cmServiceRequest = shortOption(cmServiceRequest)),
        aiCause = AiCause(
          CsCause(
            disconnection = shortOption(disconnection),
            dtmfReject = shortOption(dtmfRejectCause),
            holdRetrieveReject = shortOption(holdRetrieveReject),
            rp = shortOption(rp),
            cp = shortOption(cp),
            sequenceTerminate = shortOption(sequenceTerminate)),
          endMessage = stringOption(endMessage),
          sccpRelease = shortOption(sccpRelease),
          hoRequest = stringOption(hoRequest),
          hoRequiredReject = stringOption(hoRequiredReject),
          assignmentFailure = stringOption(assignmentFailure),
          authenticationFailure = stringOption(authenticationFailure),
          serviceCategory = stringOption(serviceCategory),
          sccpConnectionRefusal = shortOption(sccpConnectionRefusal),
          causeLocation = stringOption(causeLocation)),
        aiFlag = AiFlag(
          csFlag = CsFlag(
            ccMessage = stringOption(ccMessage),
            mmMessage = stringOption(mmMessage),
            smsMessage = stringOption(smsMessage)),
          bssmapMessage = stringOption(bssmapMessage),
          ssMessage = stringOption(ssMessage),
          sdcchDrop = stringOption(sdcchDrop)),
        csTransport = CsTransport(
          referenceNo = stringOption(referenceNo),
          protocolIdentifier = shortOption(protocolIdentifier),
          commandType = shortOption(commandType),
          status = shortOption(status),
          failureCause = shortOption(failureCause)),
        aiChannel = AiChannel(
          causeDtaprr = stringOption(causeDtaprr),
          firstChannelType = stringOption(firstChannelType),
          secondChannelType = stringOption(secondChannelType),
          firstRadioTimeSlot = stringOption(firstRadioTimeSlot),
          secondRadioTimeSlot = stringOption(secondRadioTimeSlot),
          firstArfcn = stringOption(firstArfcn),
          secondArfcn = stringOption(secondArfcn),
          srnti = stringOption(srnti),
          redirectingPartyOrServiceCentreNo = stringOption(redirectingPartyOrServiceCentreNo),
          redirectingPartyOrServiceCentreNoa = stringOption(redirectingPartyOrServiceCentreNoa)),
        csOperation = CsOperation(
          ccCode1 = intOption(ccCode1),
          ccCode2 = intOption(ccCode2),
          ssCode1 = intOption(ssCode1),
          ssCode2 = intOption(ssCode2)),
        csStatistic = CsStatistic(smsLength = stringOption(smsLength), dtmfNumberBits = stringOption(dtmfNumberBits)),
        aiConnection = AiConnection(
          csConnection = CsConnection(majorMinor.asInstanceOf[Short], stringOption(transactionId)),
          servingRncId = stringOption(servingRncId)))
    }
  }
}
