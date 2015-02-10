/*
 * TODO: License goes here!
 */

package sa.com.mobily.xdr

import scala.language.{existentials, implicitConversions}
import scala.util.Try

import org.apache.spark.sql.Row

import sa.com.mobily.event.{CsIuSource, Event}
import sa.com.mobily.parsing.{CsvParser, OpenCsvParser, RowParser}
import sa.com.mobily.user.User
import sa.com.mobily.utils.EdmCoreUtils

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
    targetCellId: Option[String]) {

  lazy val id: (Option[Int], Option[Int]) =
    if (csCell.firstLac.isDefined && !csCell.firstLac.get.isEmpty)
      if (firstSac.isDefined && !firstSac.get.isEmpty)
        (EdmCoreUtils.hexToDecimal(csCell.firstLac.get), EdmCoreUtils.hexToDecimal(firstSac.get))
      else (EdmCoreUtils.hexToDecimal(csCell.firstLac.get), None)
    else
      if (firstSac.isDefined && !firstSac.get.isEmpty) (None, Some(EdmCoreUtils.hexToInt(firstSac.get)))
      else (None, None)

  def validId: Boolean = id._1.isDefined && id._2.isDefined

  def fields: Array[String] =
    csCell.fields ++ Array(mcc.getOrElse(""), mnc.getOrElse(""), firstRac.getOrElse(""), secondRac.getOrElse(""),
      thirdRac.getOrElse(""), firstSac.getOrElse(""), secondSac.getOrElse(""), thirdSac.getOrElse(""),
      targetCellId.getOrElse(""))

  def idFields: Array[String] = Array(id._1.getOrElse("").toString, id._2.getOrElse("").toString)

  override def equals(other: Any): Boolean = other match {
    case that: IuCell => (that canEqual this) && (this.id == that.id)
    case _ => false
  }

  override def hashCode: Int = id.hashCode

  def canEqual(other: Any): Boolean = other.isInstanceOf[IuCell]
}

object IuCell {

  def header: Array[String] = CsCell.header ++
    Array("mcc", "mnc", "firstRac", "secondRac", "thirdRac", "firstSac", "secondSac", "thirdSac", "targetCellId")

  def idHeader: Array[String] = Array("Lac", "SacCellId")
}

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
      User(imei = "", imsi = user.imsi.getOrElse(""), msisdn = 0L),
      beginTime = time.csTime.begin,
      endTime = time.csTime.end,
      lacTac = cell.id._1.get,
      cellId = cell.id._2.get,
      source = CsIuSource,
      eventType = EdmCoreUtils.parseString(connection.dialogueIndicator),
      subsequentLacTac = Try { cell.csCell.secondLac.get.toInt }.toOption,
      subsequentCellId = Try { cell.secondSac.get.toInt }.toOption)
  }
}

object IuCsXdr {

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
          EdmCoreUtils.parseString(imei),
          imsi = EdmCoreUtils.parseString(imsi),
          msisdn = EdmCoreUtils.validMsisdn(callingNumber),
          imeisv = EdmCoreUtils.parseString(imeisv),
          tmsi = EdmCoreUtils.parseString(tmsi),
          oldTmsi = EdmCoreUtils.parseString(tmsiOld)),
        cell = IuCell(
          csCell = CsCell(
            firstLac = EdmCoreUtils.parseString(firstLac),
            secondLac = EdmCoreUtils.parseString(secondLac),
            thirdLac = EdmCoreUtils.parseString(thirdLac),
            oldLac = EdmCoreUtils.parseString(lacOld),
            newLac = EdmCoreUtils.parseString(lacNew)),
          mcc = EdmCoreUtils.parseString(mcc),
          mnc = EdmCoreUtils.parseString(mnc),
          firstRac = EdmCoreUtils.parseString(firstRac),
          secondRac = EdmCoreUtils.parseString(secondRac),
          thirdRac = EdmCoreUtils.parseString(thirdRac),
          firstSac = EdmCoreUtils.parseString(firstSac),
          secondSac = EdmCoreUtils.parseString(secondSac),
          thirdSac = EdmCoreUtils.parseString(thirdSac),
          targetCellId = EdmCoreUtils.parseString(targetCellId)),
        call = IuCall(
          csCall = CsCall(callType = EdmCoreUtils.parseShort(callType), calledSsn = EdmCoreUtils.parseShort(calledSsn)),
          calledNumber = EdmCoreUtils.parseString(calledNumber),
          redirectPartyOrServiceCentreNo = EdmCoreUtils.parseString(redirectPartyOrServiceCentreNo),
          calledNoa = EdmCoreUtils.parseString(calledNoa),
          callingNoa = EdmCoreUtils.parseString(callingNoa),
          redirectPartyOrServiceCentreNoA = EdmCoreUtils.parseString(redirectPartyOrServiceCentreNoA)),
        connection = IuConnection(
          csConnection = CsConnection(
            majorMinor = majorMinor.toShort,
            transactionId = EdmCoreUtils.parseString(transactionId)),
          dialogueIndicator = EdmCoreUtils.parseNullString(dialogueIndicator),
          sio = EdmCoreUtils.parseShort(sio),
          drnti = EdmCoreUtils.parseString(drnti),
          pagingAreaId = EdmCoreUtils.parseString(pagingAreaId),
          pagingRepetition = EdmCoreUtils.parseString(pagingRepetition),
          globalRncId = EdmCoreUtils.parseString(globalRncId),
          targetRncId = EdmCoreUtils.parseString(targetRncId),
          rabId = EdmCoreUtils.parseLong(rabId),
          noRabSubFlows = EdmCoreUtils.parseString(noRabSubFlows)),
        time = IuTime(
          csTime = CsTime(
            begin = EdmCoreUtils.hexToLong(startDatetime),
            end = EdmCoreUtils.hexToLong(endDateTime),
            complete = EdmCoreUtils.parseString(completeTime),
            callOnHold = EdmCoreUtils.parseString(callOnHoldTime),
            holding = EdmCoreUtils.parseString(holdingTime),
            conversation = EdmCoreUtils.parseString(conversationTime)),
          waiting = EdmCoreUtils.parseString(waitingTime),
          response = EdmCoreUtils.parseString(responseTime),
          timeout = timeout.toShort,
          violation = violation.toShort,
          transferDelay = EdmCoreUtils.parseString(transferDelay),
          rabSetup = EdmCoreUtils.parseString(rabSetupTime),
          cmServiceAccept = EdmCoreUtils.parseString(cmServiceAcceptTime)),
        ttype = IuType(
          csType = CsType(
            locationUpdate = EdmCoreUtils.parseShort(locationUpdateType),
            cmServiceRequest = EdmCoreUtils.parseShort(cmServiceRequestType)),
          iuDialogue = iuDialogueType.toShort,
          relocation = EdmCoreUtils.parseShort(relocationType)),
        localReference = IuLocalReference(
          source = EdmCoreUtils.parseLong(slr),
          destination = EdmCoreUtils.parseLong(dlr)),
        pointCode = CsPointCode(
          origin = EdmCoreUtils.parseNullString(opc),
          destination = EdmCoreUtils.parseNullString(dpc)),
        transport = IuTransport(
          csTransport = CsTransport(
            protocolIdentifier = EdmCoreUtils.parseShort(tpProtocolIdentifier),
            commandType = EdmCoreUtils.parseShort(tpCommandType),
            referenceNo = EdmCoreUtils.parseString(tpReferenceNo),
            status = EdmCoreUtils.parseShort(tpStatus),
            failureCause = EdmCoreUtils.parseShort(tpFailureCause)),
          trafficClass = EdmCoreUtils.parseShort(trafficClass),
          transportLayerAddress = EdmCoreUtils.parseString(transportLayerAddress),
          transportAssociation = EdmCoreUtils.parseString(transportAssociation)),
        operation = CsOperation(
          ccCode1 = EdmCoreUtils.parseInt(ccCode1),
          ccCode2 = EdmCoreUtils.parseInt(ccCode2),
          ssCode1 = EdmCoreUtils.parseInt(ssCode1),
          ssCode2 = EdmCoreUtils.parseInt(ssCode2)),
        flag = IuFlag(
          csFlag = CsFlag(
            ccMessage = EdmCoreUtils.parseString(ccMessageFlag),
            mmMessage = EdmCoreUtils.parseString(mmMessageFlag),
            smsMessage = EdmCoreUtils.parseString(smsMessageFlag)),
          qos = EdmCoreUtils.parseString(qosFlag),
          ranapMessage = EdmCoreUtils.parseString(ranapMessageFlag)),
        cause = IuCause(
          csCause = CsCause(
            disconnection = EdmCoreUtils.parseShort(disconnectionCause),
            dtmfReject = EdmCoreUtils.parseShort(dtmfRejectCause),
            holdRetrieveReject = EdmCoreUtils.parseShort(holdRetrieveRejectCause),
            cp = EdmCoreUtils.parseShort(cpCause),
            rp = EdmCoreUtils.parseShort(rpCause),
            sequenceTerminate = EdmCoreUtils.parseShort(sequenceTerminateCause)),
          iuRelease = EdmCoreUtils.parseShort(iuReleaseCause),
          securityReject = EdmCoreUtils.parseShort(securityRejectCause),
          paging = EdmCoreUtils.parseShort(pagingCause),
          relocation = EdmCoreUtils.parseShort(relocationCause),
          relocationFailure = EdmCoreUtils.parseShort(relocationFailureCause),
          relocationCancel = EdmCoreUtils.parseShort(relocationCancelCause),
          mmReject = EdmCoreUtils.parseShort(mmRejectCause),
          rabRelease = EdmCoreUtils.parseShort(rabReleaseCause),
          rabAssFail = EdmCoreUtils.parseShort(rabAssFailCause),
          ccRelease = EdmCoreUtils.parseShort(ccReleaseCause),
          noCli = EdmCoreUtils.parseShort(noCliCause)),
        statistic = IuStatistic(
          csStatistics = CsStatistic(
              smsLength = EdmCoreUtils.parseString(smsLength),
              dtmfNumberBits = EdmCoreUtils.parseString(dtmfNumberBits)),
          requestedDownlinkMaximumBitRate = EdmCoreUtils.parseLong(requestedDownlinkMaximumBitRate),
          requestedDownlinkGuranteedBitRate = EdmCoreUtils.parseLong(requestedDownlinkGuranteedBitRate),
          requestedUplinkMaximumBitRate = EdmCoreUtils.parseLong(requestedUplinkMaximumBitRate),
          requestedUplinkGuranteedBitRate = EdmCoreUtils.parseLong(requestedUplinkGuranteedBitRate),
          assignedDownlinkMaximumBitRate = EdmCoreUtils.parseLong(assignedDownlinkMaximumBitRate),
          assignedUplinkMaximumBitRate = EdmCoreUtils.parseLong(assignedUplinkMaximumBitRate)))
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
          imsi = EdmCoreUtils.stringOption(imsi),
          imei = EdmCoreUtils.stringOption(imei),
          msisdn = EdmCoreUtils.longOption(msisdn),
          imeisv = EdmCoreUtils.stringOption(imeisv),
          tmsi = EdmCoreUtils.stringOption(tmsi),
          oldTmsi = EdmCoreUtils.stringOption(tmsiOld)),
        cell = IuCell(
          csCell = CsCell(
            firstLac = EdmCoreUtils.stringOption(firstLac),
            secondLac = EdmCoreUtils.stringOption(secondLac),
            thirdLac = EdmCoreUtils.stringOption(thirdLac),
            oldLac = EdmCoreUtils.stringOption(lacOld),
            newLac = EdmCoreUtils.stringOption(lacNew)),
          mcc = EdmCoreUtils.stringOption(mcc),
          mnc = EdmCoreUtils.stringOption(mnc),
          firstRac = EdmCoreUtils.stringOption(firstRac),
          secondRac = EdmCoreUtils.stringOption(secondRac),
          thirdRac = EdmCoreUtils.stringOption(thirdRac),
          firstSac = EdmCoreUtils.stringOption(firstSac),
          secondSac = EdmCoreUtils.stringOption(secondSac),
          thirdSac = EdmCoreUtils.stringOption(thirdSac),
          targetCellId = EdmCoreUtils.stringOption(targetCellId)),
        call = IuCall(
          csCall = CsCall(
            callType = EdmCoreUtils.shortOption(callType),
            calledSsn = EdmCoreUtils.shortOption(calledSsn)),
          calledNumber = EdmCoreUtils.stringOption(calledNumber),
          redirectPartyOrServiceCentreNo = EdmCoreUtils.stringOption(redirectPartyOrServiceCentreNo),
          calledNoa = EdmCoreUtils.stringOption(calledNoa),
          callingNoa = EdmCoreUtils.stringOption(callingNoa),
          redirectPartyOrServiceCentreNoA = EdmCoreUtils.stringOption(redirectPartyOrServiceCentreNoA)),
        connection = IuConnection(
          csConnection = CsConnection(
            majorMinor = majorMinor.asInstanceOf[Short],
            transactionId = EdmCoreUtils.stringOption(transactionId)),
          dialogueIndicator = dialogueIndicator.asInstanceOf[String],
          sio = EdmCoreUtils.shortOption(sio),
          drnti = EdmCoreUtils.stringOption(drnti),
          pagingAreaId = EdmCoreUtils.stringOption(pagingAreaId),
          pagingRepetition = EdmCoreUtils.stringOption(pagingRepetition),
          globalRncId = EdmCoreUtils.stringOption(globalRncId),
          targetRncId = EdmCoreUtils.stringOption(targetRncId),
          rabId = EdmCoreUtils.longOption(rabId),
          noRabSubFlows = EdmCoreUtils.stringOption(noRabSubFlows)),
        time = IuTime(
          csTime = CsTime(begin = begin.asInstanceOf[Long],
            end = end.asInstanceOf[Long],
            complete = EdmCoreUtils.stringOption(complete),
            callOnHold = EdmCoreUtils.stringOption(callOnHold),
            holding = EdmCoreUtils.stringOption(holding),
            conversation = EdmCoreUtils.stringOption(conversation)),
          waiting = EdmCoreUtils.stringOption(waiting),
          response = EdmCoreUtils.stringOption(response),
          timeout = timeout.asInstanceOf[Short],
          violation = violation.asInstanceOf[Short],
          transferDelay = EdmCoreUtils.stringOption(transferDelay),
          rabSetup = EdmCoreUtils.stringOption(rabSetup),
          cmServiceAccept = EdmCoreUtils.stringOption(cmServiceAccept)),
        ttype = IuType(
          csType = CsType(
            locationUpdate = EdmCoreUtils.shortOption(locationUpdate),
            cmServiceRequest = EdmCoreUtils.shortOption(cmServiceRequest)),
          iuDialogue = iuDialogue.asInstanceOf[Short],
          relocation = EdmCoreUtils.shortOption(relocationType)),
        localReference = IuLocalReference(
          source = EdmCoreUtils.longOption(slr),
          destination = EdmCoreUtils.longOption(dlr)),
        pointCode = CsPointCode(
          origin = origin.asInstanceOf[String],
          destination = destination.asInstanceOf[String]),
        transport = IuTransport(
          csTransport = CsTransport(protocolIdentifier = EdmCoreUtils.shortOption(protocolIdentifier),
            commandType = EdmCoreUtils.shortOption(commandType),
            referenceNo = EdmCoreUtils.stringOption(referenceNo),
            status = EdmCoreUtils.shortOption(status),
            failureCause = EdmCoreUtils.shortOption(failureCause)),
          trafficClass = EdmCoreUtils.shortOption(trafficClass),
          transportLayerAddress = EdmCoreUtils.stringOption(transportLayerAddress),
          transportAssociation = EdmCoreUtils.stringOption(transportAssociation)),
        operation = CsOperation(
          ccCode1 = EdmCoreUtils.intOption(ccCode1),
          ccCode2 = EdmCoreUtils.intOption(ccCode2),
          ssCode1 = EdmCoreUtils.intOption(ssCode1),
          ssCode2 = EdmCoreUtils.intOption(ssCode2)),
        flag = IuFlag(
          csFlag = CsFlag(
            ccMessage = EdmCoreUtils.stringOption(ccMessage),
            mmMessage = EdmCoreUtils.stringOption(mmMessage),
            smsMessage = EdmCoreUtils.stringOption(smsMessage)),
          qos = EdmCoreUtils.stringOption(qos),
          ranapMessage = EdmCoreUtils.stringOption(ranapMessage)),
        cause = IuCause(
          csCause = CsCause(
            disconnection = EdmCoreUtils.shortOption(disconnection),
            dtmfReject = EdmCoreUtils.shortOption(dtmfReject),
            holdRetrieveReject = EdmCoreUtils.shortOption(holdRetrieveReject),
            cp = EdmCoreUtils.shortOption(cp),
            rp = EdmCoreUtils.shortOption(rp),
            sequenceTerminate = EdmCoreUtils.shortOption(sequenceTerminate)),
          iuRelease = EdmCoreUtils.shortOption(iuRelease),
          securityReject = EdmCoreUtils.shortOption(securityReject),
          paging = EdmCoreUtils.shortOption(paging),
          relocation = EdmCoreUtils.shortOption(relocationCause),
          relocationFailure = EdmCoreUtils.shortOption(relocationFailure),
          relocationCancel = EdmCoreUtils.shortOption(relocationCancel),
          mmReject = EdmCoreUtils.shortOption(mmReject),
          rabRelease = EdmCoreUtils.shortOption(rabReleaseCause),
          rabAssFail = EdmCoreUtils.shortOption(rabAssFailCause),
          ccRelease = EdmCoreUtils.shortOption(ccRelease),
          noCli = EdmCoreUtils.shortOption(noCli)),
        statistic = IuStatistic(
          csStatistics = CsStatistic(
            smsLength = EdmCoreUtils.stringOption(smsLength),
            dtmfNumberBits = EdmCoreUtils.stringOption(dtmfNumberBits)),
          requestedDownlinkMaximumBitRate = EdmCoreUtils.longOption(requestedDownlinkMaximumBitRate),
          requestedDownlinkGuranteedBitRate = EdmCoreUtils.longOption(requestedDownlinkGuranteedBitRate),
          requestedUplinkMaximumBitRate = EdmCoreUtils.longOption(requestedUplinkMaximumBitRate),
          requestedUplinkGuranteedBitRate = EdmCoreUtils.longOption(requestedUplinkGuranteedBitRate),
          assignedDownlinkMaximumBitRate = EdmCoreUtils.longOption(assignedDownlinkMaximumBitRate),
          assignedUplinkMaximumBitRate = EdmCoreUtils.longOption(assignedUplinkMaximumBitRate)))
    }
  }

  def isValidToBeParsedAsEvent(iuCs: IuCsXdr): Boolean = iuCs.user.imsi.isDefined && iuCs.cell.validId
}
