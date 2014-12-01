/*
 * TODO: License goes here!
 */

package sa.com.mobily.xdr

import scala.language.{existentials, implicitConversions}
import scala.util.Try

import org.apache.spark.sql.Row

import sa.com.mobily.event.Event
import sa.com.mobily.parsing.{CsvParser, OpenCsvParser, RowParser}
import sa.com.mobily.user.User
import sa.com.mobily.utils.EdmCoreUtils
import sa.com.mobily.utils.EdmCoreUtils._

final case class IuCell(
    csCell: CsCell,
    mcc: Option[String],
    mnc: Option[String],
    firstRac: Option[String],
    secondRac: Option[String],
    thirdRac: Option[String],
    firstSac: Option[String],
    secondSac: Option[String],
    thirdSac: Option[String],
    targetCellId: Option[String])

final case class IuCall(
    csCall: CsCall,
    calledNumber: Option[String],
    redirectPartyOrServiceCentreNo: Option[String],
    calledNoa: Option[String],
    callingNoa: Option[String],
    redirectPartyOrServiceCentreNoA: Option[String])

final case class IuConnection(
    csConnection: CsConnection,
    dialogueIndicator: String,
    sio: Option[Short],
    drnti: Option[String],
    pagingAreaId: Option[String],
    pagingRepetition: Option[String],
    globalRncId: Option[String],
    targetRncId: Option[String],
    rabId: Option[Long],
    noRabSubFlows: Option[String])

final case class IuTime(
    csTime: CsTime,
    waiting: Option[String],
    response: Option[String],
    timeout: Short,
    violation: Short,
    transferDelay: Option[String],
    rabSetup: Option[String],
    cmServiceAccept: Option[String])

final case class IuType(
    csType: CsType,
    iuDialogue: Short,
    relocation: Option[Short])

final case class IuLocalReference(
    source: Option[Long],
    destination: Option[Long])

final case class IuTransport(
    csTransport: CsTransport,
    trafficClass: Option[Short],
    transportLayerAddress: Option[String],
    transportAssociation: Option[String])

final case class IuFlag(
    csFlag: CsFlag,
    qos: Option[String],
    ranapMessage: Option[String])

final case class IuCause(
    csCause: CsCause,
    iuRelease: Option[Short],
    securityReject: Option[Short],
    paging: Option[Short],
    relocation: Option[Short],
    relocationFailure: Option[Short],
    relocationCancel: Option[Short],
    mmReject: Option[Short],
    rabRelease: Option[Short],
    rabAssFail: Option[Short],
    ccRelease: Option[Short],
    noCli: Option[Short])

final case class IuStatistic(
    csStatistics: CsStatistic,
    requestedDownlinkMaximumBitRate: Option[Long],
    requestedDownlinkGuranteedBitRate: Option[Long],
    requestedUplinkMaximumBitRate: Option[Long],
    requestedUplinkGuranteedBitRate: Option[Long],
    assignedDownlinkMaximumBitRate: Option[Long],
    assignedUplinkMaximumBitRate: Option[Long])

case class IuCsXdr(
    user: CsUser,
    cell: IuCell,
    call: IuCall,
    connection: IuConnection,
    time: IuTime,
    ttype: IuType,
    localReference : IuLocalReference,
    pointCode: CsPointCode,
    transport: IuTransport,
    operation: CsOperation,
    flag: IuFlag,
    cause: IuCause,
    statistic: IuStatistic) {

  def toEvent: Event = {
    Event(
      User(
        imei = user.imei.getOrElse(""),
        imsi = user.imsi.getOrElse(""),
        msisdn = user.msisdn.get.toLong),
      beginTime = hexToLong(time.csTime.begin),
      endTime = hexToLong(time.csTime.end),
      lacTac = hexToInt(cell.csCell.firstLac.get),
      cellId = hexToInt(cell.firstSac.get),
      eventType = call.csCall.callType.get.toString,
      subsequentLacTac = Try { cell.csCell.secondLac.get.toInt }.toOption,
      subsequentCellId = Try { cell.secondSac.get.toInt }.toOption)
  }
}

object IuCsXdr {

  import EdmCoreUtils._

  implicit val fromCsv = new CsvParser[IuCsXdr] {

    override def lineCsvParser: OpenCsvParser = Event.LineCsvParserObject

    override def fromFields(fields: Array[String]): IuCsXdr = { // scalastyle:ignore method.length
      val (firstChunk, remaining) = fields.splitAt(5) // scalastyle:ignore magic.number
      val (secondChunk, remaining2) = remaining.splitAt(10) // scalastyle:ignore magic.number
      val (thirdChunk, remaining3) = remaining2.splitAt(4) // scalastyle:ignore magic.number
      val (fourthChunk, remaining4) = remaining3.splitAt(2) // scalastyle:ignore magic.number
      val (fifthChunk, remaining5) = remaining4.splitAt(2) // scalastyle:ignore magic.number
      val (sixthChunk, remaining6) = remaining5.splitAt(5) // scalastyle:ignore magic.number
      val (seventhChunk, remaining7) = remaining6.splitAt(5) // scalastyle:ignore magic.number
      val (eighthChunk, remaining8) = remaining7.splitAt(4) // scalastyle:ignore magic.number
      val (ninthChunk, remaining9) = remaining8.splitAt(3) // scalastyle:ignore magic.number
      val (tenthChunk, remaining10) = remaining9.splitAt(6) // scalastyle:ignore magic.number
      val (eleventhChunk, remaining11) = remaining10.splitAt(6) // scalastyle:ignore magic.number
      val (twelfthChunk, remaining12) = remaining11.splitAt(5) // scalastyle:ignore magic.number
      val (thirteenthChunk, remaining13) = remaining12.splitAt(6) // scalastyle:ignore magic.number
      val (fourteenthChunk, remaining14) = remaining13.splitAt(7) // scalastyle:ignore magic.number
      val (fifteenthChunk, remaining15) = remaining14.splitAt(6) // scalastyle:ignore magic.number
      val (sixteenthChunk, remaining16) = remaining15.splitAt(5) // scalastyle:ignore magic.number
      val (eighteenthChunk, remaining17) = remaining16.splitAt(7) // scalastyle:ignore magic.number
      val (nineteenthChunk, remaining18) = remaining17.splitAt(5) // scalastyle:ignore magic.number
      val (twentiethChunk, twentyFirstChunk) = remaining18.splitAt(6) // scalastyle:ignore magic.number
      val Array(dialogueIndicator, iuDialogueType, startDatetime, endDateTime, sequenceTerminateCause) = firstChunk
      val Array(violation, timeout, majorMinor, opc, dpc, slr, dlr, completeTime, sio, calledSsn) = secondChunk
      val Array(transportAssociation, drnti, targetCellId, requestedDownlinkMaximumBitRate) = thirdChunk
      val Array(requestedDownlinkGuranteedBitRate, requestedUplinkMaximumBitRate) = fourthChunk
      val Array(requestedUplinkGuranteedBitRate, assignedDownlinkMaximumBitRate) = fifthChunk
      val Array(assignedUplinkMaximumBitRate, rabId, firstLac, secondLac, thirdLac) = sixthChunk
      val Array(transportLayerAddress, firstRac, secondRac, thirdRac, iuReleaseCause) = seventhChunk
      val Array(securityRejectCause, pagingCause, relocationCause, relocationType) = eighthChunk
      val Array(relocationFailureCause, relocationCancelCause, rabReleaseCause) = ninthChunk
      val Array(trafficClass, mcc, mnc, locationUpdateType, mmRejectCause, cmServiceRequestType) = tenthChunk
      val Array(responseTime, waitingTime, callOnHoldTime, ccCode1, ccCode2, callType) = eleventhChunk
      val Array(disconnectionCause, dtmfRejectCause, ccReleaseCause, noCliCause, holdRetrieveRejectCause) = twelfthChunk
      val Array(dtmfNumberBits, smsLength, tpReferenceNo, cpCause, rpCause, tpProtocolIdentifier) = thirteenthChunk
      val Array(tpCommandType, tpStatus, tpFailureCause, ssCode1, ssCode2, imsi, imei) = fourteenthChunk
      val Array(imeisv, tmsi, tmsiOld, calledNumber, callingNumber, redirectPartyOrServiceCentreNo) = fifteenthChunk
      val Array(holdingTime, conversationTime, calledNoa, callingNoa, redirectPartyOrServiceCentreNoA) = sixteenthChunk
      val Array(transactionId, lacOld, lacNew, firstSac, secondSac, ranapMessageFlag,rabAssFailCause) = eighteenthChunk
      val Array(pagingAreaId, transferDelay, rabSetupTime, cmServiceAcceptTime, ccMessageFlag) = nineteenthChunk
      val Array(pagingRepetition, globalRncId, targetRncId, qosFlag, noRabSubFlows, mmMessageFlag) = twentiethChunk
      val Array(smsMessageFlag, thirdSac, _, _, _, _, _, _, _) = twentyFirstChunk

      IuCsXdr(
        user = CsUser(
          parseString(imei),
          imsi = parseString(imsi),
          msisdn = parseString(callingNumber),
          imeisv = parseString(imeisv),
          tmsi = parseString(tmsi),
          oldTmsi = parseString(tmsiOld)),
        cell = IuCell(
          csCell = CsCell(
            firstLac = parseString(firstLac),
            secondLac = parseString(secondLac),
            thirdLac = parseString(thirdLac),
            oldLac = parseString(lacOld),
            newLac = parseString(lacNew)),
          mcc = parseString(mcc),
          mnc = parseString(mnc),
          firstRac = parseString(firstRac),
          secondRac = parseString(secondRac),
          thirdRac = parseString(thirdRac),
          firstSac = parseString(firstSac),
          secondSac = parseString(secondSac),
          thirdSac = parseString(thirdSac),
          targetCellId = parseString(targetCellId)),
        call = IuCall(
          csCall = CsCall(callType = parseShort(callType), calledSsn = parseShort(calledSsn)),
          calledNumber = parseString(calledNumber),
          redirectPartyOrServiceCentreNo = parseString(redirectPartyOrServiceCentreNo),
          calledNoa = parseString(calledNoa),
          callingNoa = parseString(callingNoa),
          redirectPartyOrServiceCentreNoA = parseString(redirectPartyOrServiceCentreNoA)),
        connection = IuConnection(
          csConnection = CsConnection(
            majorMinor = majorMinor.toShort,
            transactionId = parseString(transactionId)),
          dialogueIndicator = parseNullString(dialogueIndicator),
          sio = parseShort(sio),
          drnti = parseString(drnti),
          pagingAreaId = parseString(pagingAreaId),
          pagingRepetition = parseString(pagingRepetition),
          globalRncId = parseString(globalRncId),
          targetRncId = parseString(targetRncId),
          rabId = parseLong(rabId),
          noRabSubFlows = parseString(noRabSubFlows)),
        time = IuTime(
          csTime = CsTime(
            begin = parseNullString(startDatetime),
            end = parseNullString(endDateTime),
            complete = parseString(completeTime),
            callOnHold = parseString(callOnHoldTime),
            holding = parseString(holdingTime),
            conversation = parseString(conversationTime)),
          waiting = parseString(waitingTime),
          response = parseString(responseTime),
          timeout = timeout.toShort,
          violation = violation.toShort,
          transferDelay = parseString(transferDelay),
          rabSetup = parseString(rabSetupTime),
          cmServiceAccept = parseString(cmServiceAcceptTime)),
        ttype = IuType(
          csType = CsType(
            locationUpdate = parseShort(locationUpdateType),
            cmServiceRequest = parseShort(cmServiceRequestType)),
          iuDialogue = iuDialogueType.toShort,
          relocation = parseShort(relocationType)),
        localReference = IuLocalReference(
          source = parseLong(slr),
          destination = parseLong(dlr)),
        pointCode = CsPointCode(
          origin = parseNullString(opc),
          destination = parseNullString(dpc)),
        transport = IuTransport(
          csTransport = CsTransport(
            protocolIdentifier = parseShort(tpProtocolIdentifier),
            commandType = parseShort(tpCommandType),
            referenceNo = parseString(tpReferenceNo),
            status = parseShort(tpStatus),
            failureCause = parseShort(tpFailureCause)),
          trafficClass = parseShort(trafficClass),
          transportLayerAddress = parseString(transportLayerAddress),
          transportAssociation = parseString(transportAssociation)),
        operation = CsOperation(
          ccCode1 = parseInt(ccCode1),
          ccCode2 = parseInt(ccCode2),
          ssCode1 = parseInt(ssCode1),
          ssCode2 = parseInt(ssCode2)),
        flag = IuFlag(
          csFlag = CsFlag(
            ccMessage = parseString(ccMessageFlag),
            mmMessage = parseString(mmMessageFlag),
            smsMessage = parseString(smsMessageFlag)),
          qos = parseString(qosFlag),
          ranapMessage = parseString(ranapMessageFlag)),
        cause = IuCause(
          csCause = CsCause(
            disconnection = parseShort(disconnectionCause),
            dtmfReject = parseShort(dtmfRejectCause),
            holdRetrieveReject = parseShort(holdRetrieveRejectCause),
            cp = parseShort(cpCause),
            rp = parseShort(rpCause),
            sequenceTerminate = parseShort(sequenceTerminateCause)),
          iuRelease = parseShort(iuReleaseCause),
          securityReject = parseShort(securityRejectCause),
          paging = parseShort(pagingCause),
          relocation = parseShort(relocationCause),
          relocationFailure = parseShort(relocationFailureCause),
          relocationCancel = parseShort(relocationCancelCause),
          mmReject = parseShort(mmRejectCause),
          rabRelease = parseShort(rabReleaseCause),
          rabAssFail = parseShort(rabAssFailCause),
          ccRelease = parseShort(ccReleaseCause),
          noCli = parseShort(noCliCause)),
        statistic = IuStatistic(
          csStatistics = CsStatistic(smsLength = parseString(smsLength), dtmfNumberBits = parseString(dtmfNumberBits)),
          requestedDownlinkMaximumBitRate = parseLong(requestedDownlinkMaximumBitRate),
          requestedDownlinkGuranteedBitRate = parseLong(requestedDownlinkGuranteedBitRate),
          requestedUplinkMaximumBitRate = parseLong(requestedUplinkMaximumBitRate),
          requestedUplinkGuranteedBitRate = parseLong(requestedUplinkGuranteedBitRate),
          assignedDownlinkMaximumBitRate = parseLong(assignedDownlinkMaximumBitRate),
          assignedUplinkMaximumBitRate = parseLong(assignedUplinkMaximumBitRate)))
    }
  }

  implicit val fromRow = new RowParser[IuCsXdr] {

    override def fromRow(row: Row): IuCsXdr = { // scalastyle:ignore method.length
      val Seq(
        userRow,
        cellRow,
        callRow,
        connectionRow,
        timeRow,
        typoRow,
        localReferenceRow,
        pointCodeRow,
        transportRow,
        operationRow,
        flagRow,
        causeRow,
        statsRow) = row.asInstanceOf[Seq[Row]]
      val Seq(imei, imsi, msisdn, imeisv, tmsi, tmsiOld) = userRow
      val Seq(Seq(firstLac, secondLac, thirdLac, lacOld, lacNew),
        mcc,
        mnc,
        firstRac,
        secondRac,
        thirdRac,
        firstSac,
        secondSac,
        thirdSac,
        targetCellId) = cellRow
      val Seq(
        Seq(callType, calledSsn),
        calledNumber,
        redirectPartyOrServiceCentreNo,
        calledNoa,
        callingNoa,
        redirectPartyOrServiceCentreNoA) = callRow
      val Seq(
        Seq(majorMinor, transactionId),
        dialogueIndicator,
        sio,
        drnti,
        pagingAreaId,
        pagingRepetition,
        globalRncId,
        targetRncId,
        rabId,
        noRabSubFlows) = connectionRow
      val Seq(
        Seq(begin, end, complete, callOnHold, holding, conversation),
        waiting,
        response,
        timeout,
        violation,
        transferDelay,
        rabSetup,
        cmServiceAccept) = timeRow
      val Seq(Seq(locationUpdate, cmServiceRequest), iuDialogue, relocationType) = typoRow
      val Seq(slr, dlr) = localReferenceRow
      val Seq(origin, destination) = pointCodeRow
      val Seq(
        Seq(protocolIdentifier, commandType, referenceNo, status, failureCause),
        trafficClass,
        transportLayerAddress,
        transportAssociation) = transportRow
      val Seq(ccCode1, ccCode2, ssCode1, ssCode2) = operationRow
      val Seq(Seq(ccMessage, mmMessage, smsMessage), qos, ranapMessage) = flagRow
      val Seq(
        Seq(disconnection,dtmfReject, holdRetrieveReject, cp, rp, sequenceTerminate),
        iuRelease,
        securityReject,
        paging,
        relocationCause,
        relocationFailure,
        relocationCancel,
        mmReject,
        rabReleaseCause,
        rabAssFailCause,
        ccRelease,
        noCli) = causeRow
      val Seq(
        Seq(smsLength, dtmfNumberBits),
        requestedDownlinkMaximumBitRate,
        requestedDownlinkGuranteedBitRate,
        requestedUplinkMaximumBitRate,
        requestedUplinkGuranteedBitRate,
        assignedDownlinkMaximumBitRate,
        assignedUplinkMaximumBitRate) = statsRow

      IuCsXdr(
        user = CsUser(
          imsi = stringOption(imsi),
          imei = stringOption(imei),
          msisdn = stringOption(msisdn),
          imeisv = stringOption(imeisv),
          tmsi = stringOption(tmsi),
          oldTmsi = stringOption(tmsiOld)),
        cell = IuCell(
          csCell = CsCell(
            firstLac = stringOption(firstLac),
            secondLac = stringOption(secondLac),
            thirdLac = stringOption(thirdLac),
            oldLac = stringOption(lacOld),
            newLac = stringOption(lacNew)),
          mcc = stringOption(mcc),
          mnc = stringOption(mnc),
          firstRac = stringOption(firstRac),
          secondRac = stringOption(secondRac),
          thirdRac = stringOption(thirdRac),
          firstSac = stringOption(firstSac),
          secondSac = stringOption(secondSac),
          thirdSac = stringOption(thirdSac),
          targetCellId = stringOption(targetCellId)),
        call = IuCall(
          csCall = CsCall(callType = shortOption(callType), calledSsn = shortOption(calledSsn)),
          calledNumber = stringOption(calledNumber),
          redirectPartyOrServiceCentreNo = stringOption(redirectPartyOrServiceCentreNo),
          calledNoa = stringOption(calledNoa),
          callingNoa = stringOption(callingNoa),
          redirectPartyOrServiceCentreNoA = stringOption(redirectPartyOrServiceCentreNoA)),
        connection = IuConnection(
          csConnection = CsConnection(
            majorMinor = majorMinor.asInstanceOf[Short],
            transactionId = stringOption(transactionId)),
          dialogueIndicator = dialogueIndicator.asInstanceOf[String],
          sio = shortOption(sio),
          drnti = stringOption(drnti),
          pagingAreaId = stringOption(pagingAreaId),
          pagingRepetition = stringOption(pagingRepetition),
          globalRncId = stringOption(globalRncId),
          targetRncId = stringOption(targetRncId),
          rabId = longOption(rabId),
          noRabSubFlows = stringOption(noRabSubFlows)),
        time = IuTime(
          csTime = CsTime(begin = begin.asInstanceOf[String],
            end = end.asInstanceOf[String],
            complete = stringOption(complete),
            callOnHold = stringOption(callOnHold),
            holding = stringOption(holding),
            conversation = stringOption(conversation)),
          waiting = stringOption(waiting),
          response = stringOption(response),
          timeout = timeout.asInstanceOf[Short],
          violation = violation.asInstanceOf[Short],
          transferDelay = stringOption(transferDelay),
          rabSetup = stringOption(rabSetup),
          cmServiceAccept = stringOption(cmServiceAccept)),
        ttype = IuType(
          csType = CsType(
            locationUpdate = shortOption(locationUpdate),
            cmServiceRequest = shortOption(cmServiceRequest)),
          iuDialogue = iuDialogue.asInstanceOf[Short],
          relocation = shortOption(relocationType)),
        localReference = IuLocalReference(
          source = longOption(slr),
          destination = longOption(dlr)),
        pointCode = CsPointCode(
          origin = origin.asInstanceOf[String],
          destination = destination.asInstanceOf[String]),
        transport = IuTransport(
          csTransport = CsTransport(protocolIdentifier = shortOption(protocolIdentifier),
            commandType = shortOption(commandType),
            referenceNo = stringOption(referenceNo),
            status = shortOption(status),
            failureCause = shortOption(failureCause)),
          trafficClass = shortOption(trafficClass),
          transportLayerAddress = stringOption(transportLayerAddress),
          transportAssociation = stringOption(transportAssociation)),
        operation = CsOperation(
          ccCode1 = intOption(ccCode1),
          ccCode2 = intOption(ccCode2),
          ssCode1 = intOption(ssCode1),
          ssCode2 = intOption(ssCode2)),
        flag = IuFlag(
          csFlag = CsFlag(
            ccMessage = stringOption(ccMessage),
            mmMessage = stringOption(mmMessage),
            smsMessage = stringOption(smsMessage)),
          qos = stringOption(qos),
          ranapMessage = stringOption(ranapMessage)),
        cause = IuCause(
          csCause = CsCause(
            disconnection = shortOption(disconnection),
            dtmfReject = shortOption(dtmfReject),
            holdRetrieveReject = shortOption(holdRetrieveReject),
            cp = shortOption(cp),
            rp = shortOption(rp),
            sequenceTerminate = shortOption(sequenceTerminate)),
          iuRelease = shortOption(iuRelease),
          securityReject = shortOption(securityReject),
          paging = shortOption(paging),
          relocation = shortOption(relocationCause),
          relocationFailure = shortOption(relocationFailure),
          relocationCancel = shortOption(relocationCancel),
          mmReject = shortOption(mmReject),
          rabRelease = shortOption(rabReleaseCause),
          rabAssFail = shortOption(rabAssFailCause),
          ccRelease = shortOption(ccRelease),
          noCli = shortOption(noCli)),
        statistic = IuStatistic(
          csStatistics = CsStatistic(
            smsLength = stringOption(smsLength),
            dtmfNumberBits = stringOption(dtmfNumberBits)),
          requestedDownlinkMaximumBitRate = longOption(requestedDownlinkMaximumBitRate),
          requestedDownlinkGuranteedBitRate = longOption(requestedDownlinkGuranteedBitRate),
          requestedUplinkMaximumBitRate = longOption(requestedUplinkMaximumBitRate),
          requestedUplinkGuranteedBitRate = longOption(requestedUplinkGuranteedBitRate),
          assignedDownlinkMaximumBitRate = longOption(assignedDownlinkMaximumBitRate),
          assignedUplinkMaximumBitRate = longOption(assignedUplinkMaximumBitRate)))
    }
  }
}
