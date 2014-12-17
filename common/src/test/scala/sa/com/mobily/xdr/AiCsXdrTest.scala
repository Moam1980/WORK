/*
 * TODO: License goes here!
 */

package sa.com.mobily.xdr

import scala.language.implicitConversions

import org.apache.spark.sql.catalyst.expressions.Row
import org.scalatest.{ShouldMatchers, FlatSpec}

import sa.com.mobily.event.Event
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
          begin = "0149b9829192",
          end = "0149b982d8e6",
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
    val aiCsXdrException = aiCsXdr.copy(csUser = aiCsXdr.csUser.copy(imei = None, imsi = None, msisdn = None))

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
      "517d", null, null, null, null), Row(Row("0149b9829192", "0149b982d8e6", "424e", null, "00004330", null), null,
      null, "00004754", "00000260", "00000702", null, null, "11"), Row(null, "420034104770740", null,
      "3523870633105423", "61c5f3e5", "61c5f3e5"), Row("000392", "000146"), Row(null, "9", null, null, null, "10",
      null, null, null, null, null), Row(null, 1.toShort), Row(Row(16.toShort, null, null, null, null, 0.toShort),
      null, 0.toShort, null, null, null, null, "0", null, "0"), Row(Row("80000", "230e0000", "0"), "d8000c00", "0",
      "0"), Row(null, null, null, null, null), Row(null, null, null, null, null, null, null, null, null, null),
      Row(16, null, null, null), Row(null, null), Row(Row(0.toShort, "03"), null))
  }

  trait WithAiCsEventsToParse extends WithAiCsEvents {

    val aiCsXdrMod = aiCsXdr.copy(csUser = CsUser(
      imei = Some("42104770740"),
      imsi = Some("420034104770740"),
      msisdn = Some(352387063L),
      tmsi = Some("61c5f3e5"),
      imeisv = Some("3523870633105423"),
      oldTmsi = Some("61c5f3e5")))
    val event = Event(
      User("42104770740", "420034104770740", 352387063),
      1416156582290L,
      1416156600550L,
      2105,
      20861,
      "0",
      None,
      None,
      None,
      None,
      None)
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

  it should "be discarded when AiCsXdr is wrong" in new WithAiCsEvents {
    an[Exception] should be thrownBy aiCsXdrException.toEvent
  }
}
