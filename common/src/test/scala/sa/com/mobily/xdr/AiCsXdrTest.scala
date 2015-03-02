/*
 * TODO: License goes here!
 */

package sa.com.mobily.xdr

import scala.language.implicitConversions

import org.apache.spark.sql.catalyst.expressions.Row
import org.scalatest.{ShouldMatchers, FlatSpec}

import sa.com.mobily.event.{CsAInterfaceSource, Event}
import sa.com.mobily.parsing.CsvParser
import sa.com.mobily.user.User
import sa.com.mobily.utils.LocalSparkSqlContext

class AiCsXdrTest extends FlatSpec with ShouldMatchers with LocalSparkSqlContext {

  import AiCsXdr._

  trait WithAiCsEvents {

    val aiCsXdrLine = "0,0149b9829192,0149b982d8e6,000392,000146,0,201097345297,01,_,_,1751,1,8,21,_," +
        "420034104770740,61c5f3e5,0839,517d,_,_,_,00004330,_,_,00004754,00000260,00000702,_,_,0,_,9,_,_,_,10" +
        ",_,_,0839,517d,_,_,_,_,0839,_,_,10,_,_,03,3523870633105423,61c5f3e5,00,_,0,_,_,0,0,254,_,_,_,_,1,_,0," +
        "_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,d8000c00,230e0000,80000,0,0,16,_,_,_,_,00,0839,_,_,_,_,424e,_," +
        "0,ba,11,_,1,0,60"
    val aiCsXdrFields = Array("0", "0149b9829192", "0149b982d8e6", "000392", "000146", "0", "201097345297",
      "01", "_", "_", "1751", "1", "8", "21", "_", "420034104770740", "61c5f3e5", "0839", "517d", "_", "_", "_",
      "00004330", "_", "_", "00004754", "00000260", "00000702", "_", "_", "0", "_", "9", "_", "_", "_", "10", "_",
      "_", "0839", "517d", "_", "_", "_", "_", "0839", "_", "_", "10", "_", "_", "03", "3523870633105423",
      "61c5f3e5", "00", "_", "0", "_", "_", "0", "0", "254", "_", "_", "_", "_", "1", "_", "0", "_", "_", "_",
      "_", "_", "_", "_", "_", "_", "_", "_", "_", "_", "_", "_", "_", "_", "_", "_", "_", "d8000c00", "80000",
      "230e0000", "0", "0", "16", "_", "_", "_", "_", "00", "0839", "_", "_", "_", "_", "424e", "_", "0", "ba",
      "11", "_", "1", "0", "60")

    val aiCsXdr = AiCsXdr(
      AiCall(
        csCall = CsCall(callType = Some(0), calledSsn = Some(254)),
        scenario = "0",
        calledNo = Some("201097345297"),
        calledNai = Some("01"),
        callingNumber = None,
        callingNai = None,
        speechDividedDataIndicator = Some("1"),
        assignedChannelType = Some("8"),
        speechVersionDividedDataRate = Some("21"),
        noOfHandoverPerformed = Some("0"),
        firstHandoverReferenceNo = None,
        secondHandoverReferenceNo = None,
        isInterSystemHandoverOccurred = Some("1"),
        msPowerClass = None,
        pagingResponse = Some("0"),
        pcmMultiplexNumber = Some("ba"),
        frequencyBandUsed = Some("60"),
        imsiDetachStatus = Some("00")),
      AiCell(
        csCell = CsCell(firstLac = Some("0839"),
          secondLac = None,
          thirdLac = Some("0839"),
          newLac = Some("0839"),
          oldLac = Some("0839")),
        cic = Some("1751"),
        firstCellId = Some("517d"),
        secondCellId = None,
        thirdCellId = Some("517d"),
        servingLac = None,
        servingCellId = None,
        oldMcc = None,
        oldMnc = None),
      AiTime(
        csTime = CsTime(
          begin = 1416156582290L,
          end = 1416156600550L,
          complete = Some("424e"),
          callOnHold = None,
          holding = Some("00004330"),
          conversation = None),
        setupTime = None,
        ringingTime = None,
        completeTimeDividedSmsFirstDeliveryTime = Some("00004754"),
        assignmentTime = Some("00000260"),
        tchAssignmentTime = Some("00000702"),
        pagingTime = None,
        callSetupTime = None,
        pcmActiveTimeSlot = Some("11")),
      CsUser(
        imei = None,
        imsi = Some("420034104770740"),
        msisdn = None,
        tmsi = Some("61c5f3e5"),
        imeisv = Some("3523870633105423"),
        oldTmsi = Some("61c5f3e5")),
      CsPointCode(
        origin = "000392",
        destination = "000146"),
      AiError(
        rrCauseBssmap = None,
        causeBssmap = Some("9"),
        lcsCauseBssmap = None,
        returnErrorCauseBssmap = None,
        rejectCauseDtapmm = None,
        causeDtapcc = Some("10"),
        hoRequiredCause = None,
        hoFailureCause = None,
        assignedCipherModeRejectCause = None,
        noHandoverFailure = None,
        ssErrorCode = None),
      CsType(
        locationUpdate = None,
        cmServiceRequest = Some(1)),
      AiCause(
        csCause = CsCause(
          disconnection = Some(16),
          dtmfReject = None,
          holdRetrieveReject = None,
          rp = None,
          cp = None,
          sequenceTerminate = Some(0)),
        endMessage = None,
        sccpRelease = Some(0),
        hoRequest = None,
        hoRequiredReject = None,
        assignmentFailure = None,
        authenticationFailure = None,
        serviceCategory = Some("0"),
        sccpConnectionRefusal = None,
        causeLocation = Some("0")),
      AiFlag(
        csFlag = CsFlag(ccMessage = Some("80000"), mmMessage = Some("230e0000"), smsMessage = Some("0")),
        bssmapMessage = Some("d8000c00"),
        ssMessage = Some("0"),
        sdcchDrop = Some("0")),
      CsTransport(
        referenceNo = None,
        protocolIdentifier = None,
        commandType = None,
        status = None,
        failureCause = None),
      AiChannel(
        causeDtaprr = None,
        firstChannelType = None,
        secondChannelType = None,
        firstRadioTimeSlot = None,
        secondRadioTimeSlot = None,
        firstArfcn = None,
        secondArfcn = None,
        srnti = None,
        redirectingPartyOrServiceCentreNo = None,
        redirectingPartyOrServiceCentreNoa = None),
      CsOperation(
        ccCode1 = Some(16),
        ccCode2 = None,
        ssCode1 = None,
        ssCode2 = None),
      CsStatistic(smsLength = None, dtmfNumberBits = None),
      AiConnection(
        csConnection = CsConnection(majorMinor = 0, transactionId = Some("03")),
        servingRncId = None))
    val aiCsXdrException = aiCsXdr.copy(user = aiCsXdr.user.copy(imei = None, imsi = None, msisdn = None))

    val wrongRow = Row(Row("0", "0149b9829192", "0149b982d8e6", "0", "201097345297", "01", null, null, "1", "8",
      "21", "0", null, null, null, "1", null, "0", "ba", "60"), Row(null), Row("1751", "0839", "517d", null, null,
      "0839", "517d", "0839", null, null, null, null, 0, null, "0839"), Row(null, "00004330", null, null, "00004754",
      "00000260", "00000702", null, null, null, "424e", "11"), Row(null, null, "420034104770740", null,
      "3523870633105423", "61c5f3e5"), Row("000392", "000146"), Row(null, "9", null, null, null, "16", null, null,
      null, null, null, null, "10", "00", null, null, null), Row(null, "0"), Row(null, 0, 0, null, null, null, "1",
      null, "0", null, null, null), Row("d8000c00", "80000", "230e0000", "0", "0", "0", 254), Row(null, null, null,
      null, null), Row(null, null, null, null, null, null, null, null, null, null, "03"), Row(16, null, null, null))
    val row = Row(Row(Row(0.toShort, 254.toShort), "0", "201097345297", "01", null, null, "1", "8", "21", "0", null,
      null, "1", null, "0", "ba", "60", "00"), Row(Row("0839", null, "0839", "0839", "0839"), "1751", "517d", null,
      "517d", null, null, null, null), Row(Row(1416156582290L, 1416156600550L, "424e", null, "00004330", null), null,
      null, "00004754", "00000260", "00000702", null, null, "11"), Row(null, "420034104770740", null,
      "3523870633105423", "61c5f3e5", "61c5f3e5"), Row("000392", "000146"), Row(null, "9", null, null, null, "10",
      null, null, null, null, null), Row(null, 1.toShort), Row(Row(16.toShort, null, null, null, null, 0.toShort),
      null, 0.toShort, null, null, null, null, "0", null, "0"), Row(Row("80000", "230e0000", "0"), "d8000c00", "0",
      "0"), Row(null, null, null, null, null), Row(null, null, null, null, null, null, null, null, null, null),
      Row(16, null, null, null), Row(null, null), Row(Row(0.toShort, "03"), null))
  }

  trait WithAiCsEventsToParse extends WithAiCsEvents {

    val aiCsXdrMod = aiCsXdr.copy(user = CsUser(
        imei = Some("42104770740"),
        imsi = Some("420034104770740"),
        msisdn = Some(352387063L),
        tmsi = Some("61c5f3e5"),
        imeisv = Some("3523870633105423"),
        oldTmsi = Some("61c5f3e5")),
      cell = aiCsXdr.cell.copy(servingCellId = Some("413a")))
    val event = Event(
      User("42104770740", "420034104770740", 352387063),
      1416156582290L,
      1416156600550L,
      2105,
      20861,
      CsAInterfaceSource,
      Some("0"),
      None,
      None,
      None,
      None,
      None)

    val aiCsXdrModWithoutEventType = aiCsXdrMod.copy(call = aiCsXdrMod.call.copy(scenario = "NaN"))
    val eventWithoutEventType = event.copy(eventType = None)
  }

  trait WithAiCell {

    val cell = AiCell(
      csCell = CsCell(
        firstLac = Some("d4b"),
        secondLac = None,
        thirdLac = Some("d4b"),
        oldLac = Some("ef9"),
        newLac = Some("d4b")),
      oldMcc = Some("420"),
      oldMnc = Some("03"),
      firstCellId = Some("82eb"),
      secondCellId = Some("2001"),
      thirdCellId = Some("82eb"),
      servingLac = None,
      servingCellId = Some("AAAA"),
      cic = None)

    val cellEqual = cell.copy(oldMcc = Some("222"),
      oldMnc = Some("44"),
      secondCellId = Some("1002"),
      thirdCellId = Some("1003"),
      servingLac = Some("3001"),
      cic = None)

    val cellDistinctFirstLac = cell.copy(csCell = cell.csCell.copy(firstLac = Some("FFFF")))
    val cellDistinctFirstCellId = cell.copy(firstCellId = Some("FFFF"))

    val cellWithoutFirstLac = cell.copy(csCell = cell.csCell.copy(firstLac = None))
    val cellWithoutFirstCellId = cell.copy(firstCellId = None)
    val cellWithoutId = cellWithoutFirstLac.copy(firstCellId = None)

    val cellHeader = Array("firstLac", "secondLac", "thirdLac", "oldLac", "newLac") ++
      Array("cic", "firstCellId", "secondCellId", "thirdCellId", "servingLac", "servingCellId", "oldMcc", "oldMnc")
    val cellIdHeader = Array("firstLac", "firstCellId")

    val cellFields = Array("d4b", "", "d4b", "ef9", "d4b", "", "82eb", "2001", "82eb", "", "AAAA", "420", "03")
    val cellIdFields = Array("3403", "33515")

    val cellWithoutFirstLacIdFields = Array("", "33515")
    val cellWithoutFirstSacIdFields = Array("3403", "")
    val cellWithoutIdFields = Array("", "")
  }

  "aiCsXdr" should "be built from CSV" in new WithAiCsEvents {
    CsvParser.fromLine(aiCsXdrLine).value.get should be(aiCsXdr)
  }

  it should "be discarded when CSV format is wrong" in new WithAiCsEvents {
    an[Exception] should be thrownBy fromCsv.fromFields(aiCsXdrFields.updated(99, "NotValidField"))
  }

  it should "be built from Row" in new WithAiCsEvents {
    fromRow.fromRow(row) should be(aiCsXdr)
  }

  it should "be discarded when row is wrong" in new WithAiCsEvents {
    an[Exception] should be thrownBy fromRow.fromRow(wrongRow)
  }

  it should "parse AiCsXdr to Event" in new WithAiCsEventsToParse {
    aiCsXdrMod.toEvent should be(event)
  }

  it should "parse AiCsXdr to Event without event type" in new WithAiCsEventsToParse {
    aiCsXdrModWithoutEventType.toEvent should be(eventWithoutEventType)
  }

  it should "be discarded when AiCsXdr is wrong" in new WithAiCsEvents {
    an[Exception] should be thrownBy aiCsXdrException.toEvent
  }

  "AiCell" should "return correct header" in new WithAiCell {
    AiCell.Header should be(cellHeader)
  }

  "AiCell" should "return correct header for id" in new WithAiCell {
    AiCell.IdHeader should be(cellIdHeader)
  }

  "AiCell" should "return correct fields" in new WithAiCell {
    cell.fields should be(cellFields)
  }

  "AiCell" should "return correct fields for id" in new WithAiCell {
    cell.idFields should be(cellIdFields)
  }

  "AiCell" should "return correct fields for id without first lac" in new WithAiCell {
    cellWithoutFirstLac.idFields should be(cellWithoutFirstLacIdFields)
  }

  "AiCell" should "return correct fields for id without first cell id" in new WithAiCell {
    cellWithoutFirstCellId.idFields should be(cellWithoutFirstSacIdFields)
  }

  "AiCell" should "return correct fields for id without neither first lac nor first cell id" in new WithAiCell {
    cellWithoutId.idFields should be(cellWithoutIdFields)
  }

  "AiCell" should "return true when equals and id is the same" in new WithAiCell {
    cell == cellEqual should be(true)
    cell.equals(cellEqual) should be(true)
  }

  "AiCell" should "return false when equals and lac is different" in new WithAiCell {
    cell == cellDistinctFirstLac should be(false)
    cell.equals(cellDistinctFirstLac) should be(false)
  }

  "AiCell" should "return false when equals and sac is different" in new WithAiCell {
    cell == cellDistinctFirstLac should be(false)
    cell.equals(cellDistinctFirstLac) should be(false)
  }

  "AiCell" should "return false when equals and different objects" in new WithAiCell {
    cell == cellFields should be(false)
    cell.equals(cellFields) should be(false)
  }

  "AiCell" should "return true when checking hash codes" in new WithAiCell {
    cell.hashCode == cell.id.hashCode should be(true)
    cell.hashCode.equals(cell.id.hashCode) should be(true)
  }

  it should "return false when the user imsi is not valid to be parsed as event" in new WithAiCsEvents {
    val aiCsXdrEventWithoutImsi = aiCsXdr.copy(user = aiCsXdr.user.copy(imsi = None))
    AiCsXdr.isValidToBeParsedAsEvent(aiCsXdrEventWithoutImsi) should be(false)
  }

  it should "return false when the cell id first element is not valid to be parsed as event" in new WithAiCsEvents {
    val aiCsXdrEventWithFirstCellIdNotDefined = aiCsXdr.copy(
      cell = aiCsXdr.cell.copy(firstCellId = None))
    AiCsXdr.isValidToBeParsedAsEvent(aiCsXdrEventWithFirstCellIdNotDefined) should be(false)
  }

  it should "return false when the cell id second element is not valid to be parsed as event" in new WithAiCsEvents {
    val aiCsXdrEventWithSecondCellIdNotDefined = aiCsXdr.copy(
      cell = aiCsXdr.cell.copy(csCell = aiCsXdr.cell.csCell.copy(firstLac = None)))
    AiCsXdr.isValidToBeParsedAsEvent(aiCsXdrEventWithSecondCellIdNotDefined) should be(false)
  }

  it should "return true when the aiCsXdr is valid to be parsed as event" in new WithAiCsEvents {
    AiCsXdr.isValidToBeParsedAsEvent(aiCsXdr) should be(true)
  }
}
