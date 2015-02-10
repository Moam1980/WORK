/*
 * TODO: License goes here!
 */

package sa.com.mobily.xdr

import org.apache.spark.sql.catalyst.expressions.Row
import org.scalatest.{FlatSpec, ShouldMatchers}

import sa.com.mobily.event.{CsIuSource, Event}
import sa.com.mobily.parsing.CsvParser
import sa.com.mobily.user.User
import sa.com.mobily.utils.LocalSparkSqlContext

class IuCsXdrTest extends FlatSpec with ShouldMatchers with LocalSparkSqlContext {

  import IuCsXdr._

  trait WithIuCsXdrEvent {

    val iuCsXdrLine = "2,04,0149b9851507,0149b9851a93,0,0,0,0,9dd,22c,4939008,13173274,58c,131,142,_,_,_,_,_,_,_,_," +
      "_,_,d4b,_,d4b,_,_,_,_,83,_,_,_,_,_,_,_,_,420,03,0,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_," +
      "420032275422214,_,8636190157279614,a65ca98e,b25cb3dc,_,_,_,_,_,_,_,_,_,ef9,d4b,82eb,_,c00c0086,_,_,_,_,_,0,_," +
      "166,_,_,_,51a00200,0,82eb,_,1,_,_,_,_,_"
    val iuCsXdrFields: Array[String] = Array("2","04","0149b9851507","0149b9851a93","0","0","0","0","9dd","22c",
      "4939008","13173274","58c","131","142","_","_","_","_","_","_","_","_","_","_","d4b","_","d4b","_","_","_","_",
      "83","_","_","_","_","_","_","_","_","420","03","0","_","_","_","_","_","_","_","_","_","_","_","_","_","_","_",
      "_","_","_","_","_","_","_", "_","_","420032275422214","_","8636190157279614","a65ca98e","b25cb3dc","_","_","_",
      "_","_","_","_","_","_","ef9","d4b","82eb","_","c00c0086","_","_","_","_","_","0","_","166","_","_","_",
      "51a00200","0","82eb","_","1","_","_","_","_","_")
    val row = Row(
      Row(null, "420032275422214", null, "8636190157279614", "a65ca98e", "b25cb3dc"),
      Row(Row("d4b", null, "d4b", "ef9", "d4b"), "420", "03", null, null, null, "82eb", null, "82eb", null),
      Row(Row(null, 142.toShort), null, null, null, null, null),
      Row(Row(0.toShort, null), "2", 131.toShort, null, null, null, "166", null, null, null),
      Row(
        Row("0149b9851507", "0149b9851a93", "58c", null, null, null),
        null,
        null,
        0.toShort,
        0.toShort,
        null,
        null,
        null),
      Row(Row(0.toShort, null), 4.toShort, null),
      Row(4939008L, 13173274L),
      Row("9dd", "22c"),
      Row(Row(null, null, null, null, null), null, null, null),
      Row(null, null, null, null),
      Row(Row( "0", "51a00200", "0"), null, "c00c0086"),
      Row(
        Row(null, null, null, null, null, 0.toShort),
        83.toShort,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null),
      Row(Row(null, null), null, null, null, null, null, null))
    val wrongRow = Row(
      Row(null, "420032275422214", null, "8636190157279614", "a65ca98e", "b25cb3dc"),
      Row(Row("d4b", null, "d4b", "ef9", "d4b"), "420", "03", null, null, null, "82eb", null, "82eb", null),
      Row(Row(null, 142.toShort), null, null, null, null, null),
      Row(Row(0.toShort, null), 131.toShort, "2", null, null, null, "166", null, null, null),
      Row(
        Row("0149b9851507", "0149b9851a93", "58c", null, null, null),
        null,
        null,
        0.toShort,
        0.toShort,
        null,
        null,
        null),
      Row(Row(0.toShort, null), 4.toShort, null),
      Row(4939008L, 13173274L),
      Row("9dd", "22c"),
      Row(Row(null, null, null, null, null), null, null, null),
      Row(null, null, null, null),
      Row(Row( "0", "51a00200", "0"), null, "c00c0086"),
      Row(
        Row(null, null, null, null, null, 0.toShort),
        83.toShort,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null),
      Row(Row(null, null), null, null, null, null, null, null))
    val iuCsXdrEvent = IuCsXdr(
      user =  CsUser(
          imei = None,
          imsi = Some("420032275422214"),
          msisdn = None,
          imeisv = Some("8636190157279614"),
          tmsi = Some("a65ca98e"),
        oldTmsi = Some("b25cb3dc")),
      cell = IuCell(
        csCell = CsCell(
          firstLac = Some("d4b"),
          secondLac = None,
          thirdLac = Some("d4b"),
          oldLac = Some("ef9"),
          newLac = Some("d4b")),
        mcc = Some("420"),
        mnc = Some("03"),
        firstRac = None,
        secondRac = None,
        thirdRac = None,
        firstSac = Some("82eb"),
        secondSac = None,
        thirdSac = Some("82eb"),
        targetCellId = None),
      call = IuCall(
        csCall = CsCall(callType = None, calledSsn = Some(142)),
        calledNumber = None,
        redirectPartyOrServiceCentreNo = None,
        calledNoa = None,
        callingNoa = None,
        redirectPartyOrServiceCentreNoA = None),
      connection = IuConnection(
        csConnection = CsConnection(majorMinor = 0, transactionId = None),
        dialogueIndicator = "2",
        sio = Some(131),
        drnti = None,
        pagingAreaId = None,
        pagingRepetition = None,
        globalRncId = Some("166"),
        targetRncId = None,
        rabId = None,
        noRabSubFlows = None),
      time = IuTime(
        csTime = CsTime(
          begin = "0149b9851507",
          end = "0149b9851a93",
          complete = Some("58c"),
          callOnHold = None,
          holding = None,
          conversation = None),
        waiting = None,
        response = None,
        timeout = 0,
        violation = 0,
        transferDelay = None,
        rabSetup = None,
        cmServiceAccept = None),
      ttype = IuType(
        csType = CsType(locationUpdate = Some(0), cmServiceRequest = None),
        iuDialogue = 4,
        relocation = None),
      localReference = IuLocalReference(source = Some(4939008L), destination = Some(13173274L)),
      pointCode = CsPointCode(origin = "9dd", destination = "22c"),
      transport = IuTransport(
        csTransport = CsTransport(
          protocolIdentifier = None,
          commandType = None,
          referenceNo = None,
          status = None,
          failureCause = None),
        trafficClass = None,
        transportLayerAddress = None,
        transportAssociation = None),
      operation = CsOperation(ccCode1 = None, ccCode2 = None, ssCode1 = None, ssCode2 = None),
      flag = IuFlag(
        csFlag = CsFlag(ccMessage = Some("0"), mmMessage = Some("51a00200"), smsMessage = Some("0")),
        qos = None,
        ranapMessage = Some("c00c0086")),
      cause = IuCause(
        csCause = CsCause(
          disconnection = None,
          dtmfReject = None,
          holdRetrieveReject = None,
          cp = None,
          rp = None,
          sequenceTerminate = Some(0)),
        iuRelease = Some(83),
        securityReject = None,
        paging = None,
        relocation = None,
        relocationFailure = None,
        relocationCancel = None,
        mmReject = None,
        rabRelease = None,
        rabAssFail = None,
        ccRelease = None,
        noCli = None),
      statistic = IuStatistic(
        csStatistics = CsStatistic(smsLength = None, dtmfNumberBits = None),
        requestedDownlinkMaximumBitRate = None,
        requestedDownlinkGuranteedBitRate = None,
        requestedUplinkMaximumBitRate = None,
        requestedUplinkGuranteedBitRate = None,
        assignedDownlinkMaximumBitRate = None,
        assignedUplinkMaximumBitRate = None))
    val iuCsXdrEventWrong = iuCsXdrEvent.copy(time = iuCsXdrEvent.time.copy(
      csTime = iuCsXdrEvent.time.csTime.copy(begin = "")))
    val iuCsXdrEvent2 = iuCsXdrEvent.copy(
      user = iuCsXdrEvent.user.copy(
        imei = Some("8636190157279614"), imsi = Some("420032275422214"), msisdn = Some(666666666L)),
      call = iuCsXdrEvent.call.copy(
        csCall = iuCsXdrEvent.call.csCall.copy(
          callType = Some(0))))
    val event = Event(
      User("8636190157279614", "420032275422214", 666666666),
      1416156747015L,
      1416156748435L,
      3403,
      33515,
      CsIuSource,
      Some("2"),
      None,
      None,
      None,
      None,
      None)
  }

  trait WithIuCell {

    val cell = IuCell(
      csCell = CsCell(
        firstLac = Some("d4b"),
        secondLac = None,
        thirdLac = Some("d4b"),
        oldLac = Some("ef9"),
        newLac = Some("d4b")),
      mcc = Some("420"),
      mnc = Some("03"),
      firstRac = None,
      secondRac = None,
      thirdRac = None,
      firstSac = Some("82eb"),
      secondSac = None,
      thirdSac = Some("82eb"),
      targetCellId = None)

    val cellEqual = cell.copy(mcc = Some("222"),
      mnc = Some("44"),
      firstRac = Some("0001"),
      secondRac = Some("0002"),
      thirdRac = Some("0003"),
      secondSac = Some("1002"),
      thirdSac = Some("1003"),
      targetCellId = Some("1111"))

    val cellDistinctFirstLac = cell.copy(csCell = cell.csCell.copy(firstLac = Some("FFFF")))
    val cellDistinctFirstSac = cell.copy(firstSac = Some("FFFF"))

    val cellWithoutFirstLac = cell.copy(csCell = cell.csCell.copy(firstLac = None))
    val cellWithoutFirstSac = cell.copy(firstSac = None)
    val cellWithoutId = cellWithoutFirstLac.copy(firstSac = None)

    val cellHeader = Array("firstLac", "secondLac", "thirdLac", "oldLac", "newLac") ++
      Array("mcc", "mnc", "firstRac", "secondRac", "thirdRac", "firstSac", "secondSac", "thirdSac", "targetCellId")
    val cellIdHeader = Array("Lac", "SacCellId")

    val cellFields = Array("d4b", "", "d4b", "ef9", "d4b", "420", "03", "", "", "", "82eb", "", "82eb", "")
    val cellIdFields = Array("3403", "33515")

    val cellWithoutFirstLacIdFields = Array("", "33515")
    val cellWithoutFirstSacIdFields = Array("3403", "")
    val cellWithoutIdFields = Array("", "")
  }

  "IuCsXdr" should "be built from CSV" in new WithIuCsXdrEvent {
    CsvParser.fromLine(iuCsXdrLine).value.get should be (iuCsXdrEvent)
  }

  it should "be discarded when CSV format is wrong" in new WithIuCsXdrEvent {
    an [Exception] should be thrownBy fromCsv.fromFields(iuCsXdrFields.updated(1, "A"))
  }

  it should "be built from Row" in new WithIuCsXdrEvent {
    fromRow.fromRow(row) should be (iuCsXdrEvent)
  }

  it should "be discarded when row is wrong" in new WithIuCsXdrEvent {
    an[Exception] should be thrownBy fromRow.fromRow(wrongRow)
  }

  it should "be parsed to Event" in new WithIuCsXdrEvent {
    iuCsXdrEvent2.toEvent should be (event)
  }

  it should "be discarded when IuCsXdr is wrong" in new WithIuCsXdrEvent {
    an[Exception] should be thrownBy iuCsXdrEventWrong.toEvent
  }

  "IuCell" should "return correct header" in new WithIuCell {
    IuCell.header should be (cellHeader)
  }

  it should "return correct header for id" in new WithIuCell {
    IuCell.idHeader should be (cellIdHeader)
  }

  it should "return correct fields" in new WithIuCell {
    cell.fields should be (cellFields)
  }

  it should "return correct fields for id" in new WithIuCell {
    cell.idFields should be (cellIdFields)
  }

  it should "return correct fields for id without first lac" in new WithIuCell {
    cellWithoutFirstLac.idFields should be (cellWithoutFirstLacIdFields)
  }

  it should "return correct fields for id without first sac" in new WithIuCell {
    cellWithoutFirstSac.idFields should be (cellWithoutFirstSacIdFields)
  }

  it should "return correct fields for id without neither first lac nor first sac" in new WithIuCell {
    cellWithoutId.idFields should be (cellWithoutIdFields)
  }

  it should "return true when equals and id is the same" in new WithIuCell {
    cell == cellEqual should be (true)
    cell.equals(cellEqual) should be (true)
  }

  it should "return false when equals and lac is different" in new WithIuCell {
    cell == cellDistinctFirstLac should be (false)
    cell.equals(cellDistinctFirstLac) should be (false)
  }

  it should "return false when equals and sac is different" in new WithIuCell {
    cell == cellDistinctFirstSac should be (false)
    cell.equals(cellDistinctFirstSac) should be (false)
  }

  it should "return false when equals and different objects" in new WithIuCell {
    cell == cellFields should be (false)
    cell.equals(cellFields) should be (false)
  }

  it should "return true when checking hash codes" in new WithIuCell {
    cell.hashCode  == cell.id.hashCode should be (true)
    cell.hashCode.equals(cell.id.hashCode) should be (true)
  }

  it should "return false when the user imsi is not valid to be parsed as event" in new WithIuCsXdrEvent {
    val iuCsXdrEventWithoutImsi = iuCsXdrEvent.copy(user = iuCsXdrEvent.user.copy(imsi = None))
    IuCsXdr.isValidToBeParsedAsEvent(iuCsXdrEventWithoutImsi) should be(false)
  }

  it should "return false when the begin time is not valid to be parsed as event" in new WithIuCsXdrEvent {
    val iuCsXdrEventWithEmptyBeginTime = iuCsXdrEvent.copy(
      time = iuCsXdrEvent.time.copy(csTime = iuCsXdrEvent.time.csTime.copy(begin = "")))
    IuCsXdr.isValidToBeParsedAsEvent(iuCsXdrEventWithEmptyBeginTime) should be(false)
  }

  it should "return false when the end time is not valid to be parsed as event" in new WithIuCsXdrEvent {
    val iuCsXdrEventWithEmptyBeginTime = iuCsXdrEvent.copy(
      time = iuCsXdrEvent.time.copy(csTime = iuCsXdrEvent.time.csTime.copy(end = "")))
    IuCsXdr.isValidToBeParsedAsEvent(iuCsXdrEventWithEmptyBeginTime) should be(false)
  }

  it should "return false when the cell id first element is not valid to be parsed as event" in new WithIuCsXdrEvent {
    val iuCsXdrEventWithFirstSacNotDefined = iuCsXdrEvent.copy(
      cell = iuCsXdrEvent.cell.copy(firstSac = None))
    IuCsXdr.isValidToBeParsedAsEvent(iuCsXdrEventWithFirstSacNotDefined) should be(false)
  }

  it should "return false when the cell id second element is not valid to be parsed as event" in new WithIuCsXdrEvent {
    val iuCsXdrEventWithFirstSacNotDefined = iuCsXdrEvent.copy(
      cell = iuCsXdrEvent.cell.copy(csCell = iuCsXdrEvent.cell.csCell.copy(firstLac = None)))
    IuCsXdr.isValidToBeParsedAsEvent(iuCsXdrEventWithFirstSacNotDefined) should be(false)
  }

  it should "return true when the iuCsXdrEvent is valid to be parsed as event" in new WithIuCsXdrEvent {
    IuCsXdr.isValidToBeParsedAsEvent(iuCsXdrEvent) should be(true)
  }
}
