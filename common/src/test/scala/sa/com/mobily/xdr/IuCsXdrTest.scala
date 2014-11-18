/*
 * TODO: License goes here!
 */

package sa.com.mobily.xdr

import org.apache.spark.sql.catalyst.expressions.Row
import org.scalatest.{FlatSpec, ShouldMatchers}

import sa.com.mobily.parsing.CsvParser
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
      Row(Row(null, "420032275422214", null, "8636190157279614", "a65ca98e"), "b25cb3dc"),
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
      Row(Row(null), null, null, null, null, null, null, null))
    val wrongRow = Row(
      Row(Row(null, "420032275422214", null, "8636190157279614", "a65ca98e"), "b25cb3dc"),
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
      Row(Row(null), null, null, null, null, null, null, null))
    val iuCsXdrEvent = IuCsXdr(
      user = IuUser(
        csUser = CsUser(
          imei = None,
          imsi = Some("420032275422214"),
          msisdn = None,
          imeisv = Some("8636190157279614"),
          tmsi = Some("a65ca98e")),
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
          sequenceTerminate = 0),
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
        csStatistics = CsStatistic(smsLength = None),
        requestedDownlinkMaximumBitRate = None,
        requestedDownlinkGuranteedBitRate = None,
        requestedUplinkMaximumBitRate = None,
        requestedUplinkGuranteedBitRate = None,
        assignedDownlinkMaximumBitRate = None,
        assignedUplinkMaximumBitRate = None,
        dtmfNumberBits = None))
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
}
