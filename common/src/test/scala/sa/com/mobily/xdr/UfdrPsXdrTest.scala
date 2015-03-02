/*
 * TODO: License goes here!
 */

package sa.com.mobily.xdr

import org.apache.spark.sql.catalyst.expressions.Row
import org.scalatest.{FlatSpec, ShouldMatchers}

import sa.com.mobily.event.{PsUfdrSource, Event}
import sa.com.mobily.parsing.CsvParser
import sa.com.mobily.user.User

class UfdrPsXdrTest extends FlatSpec with ShouldMatchers {

  import UfdrPsXdr._

  trait WithUfdrPsXdr {

    val line = "11692618241|1|1414184401|1414184401|1|1906|200912053883|420034103554735|357940040696441|" +
      "100.114.249.146|207.46.194.10|56194|80|WEB2|84.23.103.137|84.23.98.115||84.23.98.97|10.201.55.114|0|0FE7||" +
      "AF88||||504|40|1|1||0|c.bing.com|c.bing.com/c.gif?anx_uid=4894933205928070566&red3=msan_pd|" +
      "Mozilla/5.0(WindowsNT6.1;WOW64)AppleWebKit/537.36(KHTML-likeGecko)Chrome/38.0.2125.104Safari/537.36|" +
      "97|98||||420|03"
    val fields = Array("11692618241", "1", "1414184401", "1414184401", "1", "1906", "200912053883", "420034103554735",
      "357940040696441", "100.114.249.146", "207.46.194.10", "56194", "80", "WEB2", "84.23.103.137","84.23.98.115", "",
      "84.23.98.97", "10.201.55.114", "0", "0FE7", "", "AF88", "", "", "", "504", "40", "1", "1", "", "0", "c.bing.com",
      "c.bing.com/c.gif?anx_uid=4894933205928070566&red3=msan_pd",
      "Mozilla/5.0(WindowsNT6.1;WOW64)AppleWebKit/537.36(KHTML-likeGecko)Chrome/38.0.2125.104Safari/537.36",
      "97","98", "", "", "", "420","03")

    val row = Row(11692618241L, Row(Gn.identifier), Row(1414184401L * 1000, 1414184401L * 1000),
      Row(Row(P2P.identifier), 1906), Row("357940040696441", "420034103554735", 200912053883L),
      Row("100.114.249.146", 56194), Row("207.46.194.10", 80), "WEB2", Row("84.23.103.137", ""),
      Row("84.23.98.115", "84.23.98.97"), "10.201.55.114",
      Row(Row(Null.identifier), "0FE7", "", "AF88", "", "", "", "420", "03"),
      Row(504L, 40L, 1, 1, 0L, 0L, 0, 0),
      "c.bing.com", "c.bing.com/c.gif?anx_uid=4894933205928070566&red3=msan_pd",
      "Mozilla/5.0(WindowsNT6.1;WOW64)AppleWebKit/537.36(KHTML-likeGecko)" +
        "Chrome/38.0.2125.104Safari/537.36",
      Row(97, 98), None)
    val wrongRow = Row(11692618241L, Row(Gn.identifier), Row(1414184401L * 1000, 1414184401L * 1000),
      Row(Row(P2P.identifier), 1906), Row(200912053883L, "420034103554735", "357940040696441"),
      Row("100.114.249.146", 56194), Row("207.46.194.10", 80), "WEB2", Row("84.23.103.137", ""),
      Row("84.23.98.115", "84.23.98.97"), "10.201.55.114", Row(Null, "0FE7", "", "AF88", "", "", "", "420", "03"),
      Row(504L, 40L, 1, 1, 0L, Some(0L), 0, 0), Some("c.bing.com"),
      Some("c.bing.com/c.gif?anx_uid=4894933205928070566&red3=msan_pd"),
      Some("Mozilla/5.0(WindowsNT6.1;WOW64)AppleWebKit/537.36(KHTML-likeGecko)" +
        "Chrome/38.0.2125.104Safari/537.36"),
      Row(97, 98), None)

    val protocol = Protocol(
      category = P2P,
      id = 1906)
    val protocolArray = Array(
      P2P.identifier.toString,
      "1906")
    val user = User(
      imei = "357940040696441",
      imsi = "420034103554735",
      msisdn = 200912053883L)
    val userArray = Array("357940040696441", "420034103554735", "200912053883")

    val ufdrCell = UfdrPSXdrCell(
      rat = Null,
      lac = "0FE7",
      rac = "",
      sac = "AF88",
      ci = "",
      tac = "",
      eci = "",
      mcc = "420",
      mnc = "03")
    val ufdrCellArray = Array(Null.identifier.toString, "0FE7", "", "AF88", "", "", "", "420", "03")

    val ufdrCellIdentifier = (4071, 44936)
    val ufdrCellIdentifierArray = Array("4071", "44936")

    val transferStats = TransferStats(
      l4UlThroughput = 504L,
      l4DwThroughput = 40L,
      l4UlPackets = 1,
      l4DwPackets = 1,
      dataTransUlDuration = 0L,
      dataTransDwDuration = 0L,
      ulLostRate = 0,
      dwLostRate = 0)
    val transferStatsArray = Array("504", "40", "1", "1", "0", "0", "0", "0")

    val ufdrPsXdr = UfdrPsXdr(
      sid = 11692618241L,
      interfaceId = Gn,
      duration = Duration(beginTime = 1414184401L * 1000, endTime = 1414184401L * 1000),
      protocol = protocol,
      user = user,
      msInet = Inet(
        ip = "100.114.249.146",
        port = 56194),
      serverInet = Inet(
        ip = "207.46.194.10",
        port = 80),
      apn = "WEB2",
      sgsnNetworkNode = NetworkNode(
        sigIp = "84.23.103.137",
        userIp = ""),
      ggsnNetworkNode = NetworkNode(
        sigIp = "84.23.98.115",
        userIp = "84.23.98.97"),
      ranNeUserIp = "10.201.55.114",
      cell = ufdrCell,
      transferStats = transferStats,
      host = Some("c.bing.com"),
      firstUri = Some("c.bing.com/c.gif?anx_uid=4894933205928070566&red3=msan_pd"),
      userAgent =
        Some("Mozilla/5.0(WindowsNT6.1;WOW64)AppleWebKit/537.36(KHTML-likeGecko)Chrome/38.0.2125.104Safari/537.36"),
      durationMsel = DurationMsel(beginTimeMsel = 97, endTimeMsel = 98),
      clickToContent = None)

    val cellHeader = Array("rat", "lac", "rac", "sac", "ci", "tac", "eci", "mcc", "mnc")
    val cellIdentifierHeader = Array("LacTac", "SacEci")
    val userHeader = Array("imei", "imsi", "msisdn")
    val transferStatsHeader = Array(
      "l4UlThroughput",
      "l4DwThroughput",
      "l4UlPackets",
      "l4DwPackets",
      "dataTransUlDuration",
      "dataTransDwDuration",
      "ulLostRate",
      "dwLostRate")
    val hierarchyHeader =
      Array("Date Hour") ++ cellIdentifierHeader ++ userHeader ++ Array("category id", "protocol id")
    val hierarchyAggHeader = hierarchyHeader ++ transferStatsHeader

    val ufdrPsXdrHierarchy = UfdrPsXdrHierarchy(
      hourTime = "2014/10/25 01:01:00",
      cell = ufdrCell,
      user = user,
      protocol = protocol)
    val ufdrPsXdrHierarchyAgg = UfdrPsXdrHierarchyAgg(hierarchy = ufdrPsXdrHierarchy, transferStats = transferStats)

    val ufdrPsXdrHierarchyArray =
      Array("2014/10/25 01:01:00") ++
        ufdrCellIdentifierArray ++
        userArray ++
        protocolArray
    val ufdrPsXdrHierarchyAggArray = ufdrPsXdrHierarchyArray ++ transferStatsArray

    val ufdrPsXdrMod = ufdrPsXdr.copy(cell = ufdrCell.copy( sac = ""))

    val event = Event(
      user,
      beginTime = 1414184401L * 1000,
      endTime = 1414184401L * 1000,
      lacTac = 4071,
      cellId = 44936,
      source = PsUfdrSource,
      eventType = Some(protocol.category.identifier + "." + protocol.id),
      subsequentLacTac = None,
      subsequentCellId = None)

    val ufdrPsXdrTacEci = ufdrPsXdr.copy(cell = ufdrCell.copy(lac = "", tac = "0F0F", sac = "", eci = "F0F0"))

    val eventTacEci = Event(
      user,
      beginTime = 1414184401L * 1000,
      endTime = 1414184401L * 1000,
      lacTac = 3855,
      cellId = 61680,
      source = PsUfdrSource,
      eventType = Some(protocol.category.identifier + "." + protocol.id),
      subsequentLacTac = None,
      subsequentCellId = None)
  }

  trait WithEqualityCells {

    val ufdrCellLacSac = UfdrPSXdrCell(
      rat = Null,
      lac = "0FE7",
      rac = "",
      sac = "AF88",
      ci = "",
      tac = "",
      eci = "",
      mcc = "420",
      mnc = "03")
    val ufdrCellLacSacId = (4071, 44936)
    val ufdrCellLac = ufdrCellLacSac.copy(sac = "")
    val ufdrCellLacId = (4071, -1)
    val ufdrCellLacSacSame = UfdrPSXdrCell(
      rat = Utran,
      lac = "0FE7",
      rac = "",
      sac = "AF88",
      ci = "",
      tac = "",
      eci = "",
      mcc = "",
      mnc = "")
    val ufdrCellLacSacSameId = (4071, 44936)
    val ufdrCellSecondLac = ufdrCellLacSac.copy(lac = "0F0F")
    val ufdrCellSecondLacId = (3855, 44936)
    val ufdrCellSecondSac = ufdrCellLacSac.copy(sac = "F0F0")
    val ufdrCellSecondSacId = (4071, 61680)

    val ufdrCellTacEci = UfdrPSXdrCell(
      rat = Null,
      lac = "",
      rac = "",
      sac = "",
      ci = "",
      tac = "0FE7",
      eci = "AF88",
      mcc = "420",
      mnc = "03")
    val ufdrCellTacEciId = (4071, 44936)
    val ufdrCellTac = ufdrCellTacEci.copy(eci = "")
    val ufdrCellTacId = (4071, -1)
    val ufdrCellTacCi = ufdrCellTacEci.copy(eci = "", ci = "AAAA")
    val ufdrCellTacCiId = (4071, 43690)
    val ufdrCellTacEciSame = UfdrPSXdrCell(
      rat = Utran,
      lac = "",
      rac = "",
      sac = "",
      ci = "",
      tac = "0FE7",
      eci = "AF88",
      mcc = "",
      mnc = "")
    val ufdrCellTacEciSameId = (4071, 44936)
    val ufdrCellSecondTac = ufdrCellTacEci.copy(tac = "0F0F")
    val ufdrCellSecondTacId = (3855, 44936)
    val ufdrCellSecondEci = ufdrCellTacEci.copy(eci = "F0F0")
    val ufdrCellSecondEciId = (4071, 61680)

    val ufdrCellDifferentObject = Array("0FE7", "AF88")

    val ufdrCellEmpty = UfdrPSXdrCell(
      rat = Utran,
      lac = "",
      rac = "",
      sac = "",
      ci = "",
      tac = "",
      eci = "",
      mcc = "",
      mnc = "")
    val ufdrCellEmptyId = (-1, -1)
  }

  "UfdrPsXdr" should "be built from CSV" in new WithUfdrPsXdr {
    CsvParser.fromLine(line).value.get should be (ufdrPsXdr)
  }

  it should "be discarded when the CSV format is wrong" in new WithUfdrPsXdr {
    an [Exception] should be thrownBy fromCsv.fromFields(fields.updated(0, "NaN"))
  }

  it should "be built from Row" in new WithUfdrPsXdr {
    fromRow.fromRow(row) should be (ufdrPsXdr)
  }

  it should "be discarded when row is wrong" in new WithUfdrPsXdr {
    an[Exception] should be thrownBy fromRow.fromRow(wrongRow)
  }

  it should "be built from CSV with IuPs interface" in new WithUfdrPsXdr {
    fromCsv.fromFields(fields.updated(1, "3")) should be (ufdrPsXdr.copy(interfaceId = IuPs))
  }

  it should "be built from CSV with Gb interface" in new WithUfdrPsXdr {
    fromCsv.fromFields(fields.updated(1, "4")) should be (ufdrPsXdr.copy(interfaceId = Gb))
  }

  it should "be built from CSV with S11 interface" in new WithUfdrPsXdr {
    fromCsv.fromFields(fields.updated(1, "5")) should be (ufdrPsXdr.copy(interfaceId = S11))
  }

  it should "be built from CSV with S5S8 interface" in new WithUfdrPsXdr {
    fromCsv.fromFields(fields.updated(1, "6")) should be (ufdrPsXdr.copy(interfaceId = S5S8))
  }

  it should "be built from CSV with S1Mme interface" in new WithUfdrPsXdr {
    fromCsv.fromFields(fields.updated(1, "7")) should be (ufdrPsXdr.copy(interfaceId = S1Mme))
  }

  it should "be built from CSV with S6a interface" in new WithUfdrPsXdr {
    fromCsv.fromFields(fields.updated(1, "8")) should be (ufdrPsXdr.copy(interfaceId = S6a))
  }

  it should "be built from CSV with Im ProtocolCategory" in new WithUfdrPsXdr {
    fromCsv.fromFields(fields.updated(4, "2")) should
      be (ufdrPsXdr.copy(protocol = ufdrPsXdr.protocol.copy(category = Im)))
  }

  it should "be built from CSV with VoIp ProtocolCategory" in new WithUfdrPsXdr {
    fromCsv.fromFields(fields.updated(4, "3")) should
      be (ufdrPsXdr.copy(protocol = ufdrPsXdr.protocol.copy(category = VoIp)))
  }

  it should "be built from CSV with WebBrowsing ProtocolCategory" in new WithUfdrPsXdr {
    fromCsv.fromFields(fields.updated(4, "4")) should
      be (ufdrPsXdr.copy(protocol = ufdrPsXdr.protocol.copy(category = WebBrowsing)))
  }

  it should "be built from CSV with Game ProtocolCategory" in new WithUfdrPsXdr {
    fromCsv.fromFields(fields.updated(4, "5")) should
      be (ufdrPsXdr.copy(protocol = ufdrPsXdr.protocol.copy(category = Game)))
  }

  it should "be built from CSV with Streaming ProtocolCategory" in new WithUfdrPsXdr {
    fromCsv.fromFields(fields.updated(4, "6")) should
      be (ufdrPsXdr.copy(protocol = ufdrPsXdr.protocol.copy(category = Streaming)))
  }

  it should "be built from CSV with Email ProtocolCategory" in new WithUfdrPsXdr {
    fromCsv.fromFields(fields.updated(4, "9")) should
      be (ufdrPsXdr.copy(protocol = ufdrPsXdr.protocol.copy(category = Email)))
  }

  it should "be built from CSV with FileAccess ProtocolCategory" in new WithUfdrPsXdr {
    fromCsv.fromFields(fields.updated(4, "10")) should
      be (ufdrPsXdr.copy(protocol = ufdrPsXdr.protocol.copy(category = FileAccess)))
  }

  it should "be built from CSV with NetworkStorage ProtocolCategory" in new WithUfdrPsXdr {
    fromCsv.fromFields(fields.updated(4, "12")) should
      be (ufdrPsXdr.copy(protocol = ufdrPsXdr.protocol.copy(category  = NetworkStorage)))
  }

  it should "be built from CSV with Stock ProtocolCategory" in new WithUfdrPsXdr {
    fromCsv.fromFields(fields.updated(4, "15")) should
      be (ufdrPsXdr.copy(protocol = ufdrPsXdr.protocol.copy(category = Stock)))
  }

  it should "be built from CSV with Tunnelling ProtocolCategory" in new WithUfdrPsXdr {
    fromCsv.fromFields(fields.updated(4, "16")) should
      be (ufdrPsXdr.copy(protocol = ufdrPsXdr.protocol.copy(category = Tunnelling)))
  }

  it should "be built from CSV with Miscellaneous ProtocolCategory" in new WithUfdrPsXdr {
    fromCsv.fromFields(fields.updated(4, "17")) should
      be (ufdrPsXdr.copy(protocol = ufdrPsXdr.protocol.copy(category = Miscellaneous)))
  }

  it should "be built from CSV with SocialNetworking ProtocolCategory" in new WithUfdrPsXdr {
    fromCsv.fromFields(fields.updated(4, "18")) should
      be (ufdrPsXdr.copy(protocol = ufdrPsXdr.protocol.copy(category = SocialNetworking)))
  }

  it should "be built from CSV with SoftwareUpdate ProtocolCategory" in new WithUfdrPsXdr {
    fromCsv.fromFields(fields.updated(4, "19")) should
      be (ufdrPsXdr.copy(protocol = ufdrPsXdr.protocol.copy(category = SoftwareUpdate)))
  }

  it should "be built from CSV with Utran RadioAccessTechnology" in new WithUfdrPsXdr {
    fromCsv.fromFields(fields.updated(19, "1")) should be (ufdrPsXdr.copy(cell = ufdrPsXdr.cell.copy(rat = Utran)))
  }

  it should "be built from CSV with Geran RadioAccessTechnology" in new WithUfdrPsXdr {
    fromCsv.fromFields(fields.updated(19, "2")) should be (ufdrPsXdr.copy(cell = ufdrPsXdr.cell.copy(rat = Geran)))
  }

  it should "be built from CSV with WLan RadioAccessTechnology" in new WithUfdrPsXdr {
    fromCsv.fromFields(fields.updated(19, "3")) should be (ufdrPsXdr.copy(cell = ufdrPsXdr.cell.copy(rat = WLan)))
  }

  it should "be built from CSV with Gan RadioAccessTechnology" in new WithUfdrPsXdr {
    fromCsv.fromFields(fields.updated(19, "4")) should be (ufdrPsXdr.copy(cell = ufdrPsXdr.cell.copy(rat = Gan)))
  }

  it should "be built from CSV with HspaEvolution RadioAccessTechnology" in new WithUfdrPsXdr {
    fromCsv.fromFields(fields.updated(19, "5")) should
      be (ufdrPsXdr.copy(cell = ufdrPsXdr.cell.copy(rat = HspaEvolution)))
  }

  it should "be built from CSV with EuTran RadioAccessTechnology" in new WithUfdrPsXdr {
    fromCsv.fromFields(fields.updated(19, "6")) should be (ufdrPsXdr.copy(cell = ufdrPsXdr.cell.copy(rat = EuTran)))
  }

  it should "generate header for cell" in new WithUfdrPsXdr {
    UfdrPSXdrCell.Header should be (cellHeader)
  }

  it should "generate header for cell identifier" in new WithUfdrPsXdr {
    UfdrPSXdrCell.idHeader should be (cellIdentifierHeader)
  }

  it should "generate string array for cell" in new WithUfdrPsXdr {
    ufdrCell.fields should be (ufdrCellArray)
  }

  it should "generate string array for cell identifier" in new WithUfdrPsXdr {
    ufdrCell.idFields should be (ufdrCellIdentifierArray)
  }

  it should "generate header for hierarchy" in new WithUfdrPsXdr {
    UfdrPsXdrHierarchy.Header should be (hierarchyHeader)
  }

  it should "generate header for hierarchy aggregation" in new WithUfdrPsXdr {
    UfdrPsXdrHierarchyAgg.Header should be (hierarchyAggHeader)
  }

  it should "generate string array for hierarchy" in new WithUfdrPsXdr {
    ufdrPsXdrHierarchy.fields should be (ufdrPsXdrHierarchyArray)
  }

  it should "generate string array for hierarchy aggregation" in new WithUfdrPsXdr {
    ufdrPsXdrHierarchyAgg.fields should be (ufdrPsXdrHierarchyAggArray)
  }

  it should "parse UfdrPsXdr to Event" in new WithUfdrPsXdr {
    ufdrPsXdr.toEvent should be(event)
  }

  it should "parse UfdrPsXdr to Event with TAC Eci" in new WithUfdrPsXdr {
    ufdrPsXdrTacEci.toEvent should be(eventTacEci)
  }

  it should "compare to true two cells with the same lac/tac sac/eci" in new WithEqualityCells {
    ufdrCellLacSac == ufdrCellLacSacSame should be(true)
    ufdrCellLacSac.equals(ufdrCellLacSacSame) should be(true)

    ufdrCellTacEci == ufdrCellTacEciSame should be(true)
    ufdrCellTacEci.equals(ufdrCellTacEciSame) should be(true)
  }

  it should "compare to false two cells with different lac/tac" in new WithEqualityCells {
    ufdrCellLacSac == ufdrCellSecondLac should be(false)
    ufdrCellLacSac.equals(ufdrCellSecondLac) should be(false)

    ufdrCellLacSac == ufdrCellSecondSac should be(false)
    ufdrCellLacSac.equals(ufdrCellSecondSac) should be(false)

    ufdrCellTacEci == ufdrCellSecondTac should be(false)
    ufdrCellTacEci.equals(ufdrCellSecondTac) should be(false)

    ufdrCellTacEci == ufdrCellSecondEci should be(false)
    ufdrCellTacEci.equals(ufdrCellSecondEci) should be(false)
  }

  it should "compare to false two different objects" in new WithEqualityCells {
    ufdrCellLacSac == ufdrCellDifferentObject should be(false)
    ufdrCellLacSac.equals(ufdrCellDifferentObject) should be(false)

    ufdrCellTacEci == ufdrCellDifferentObject should be(false)
    ufdrCellTacEci.equals(ufdrCellDifferentObject) should be(false)
  }

  it should "return lac/sac as identifier" in new WithEqualityCells {
    ufdrCellLacSac.id should be(ufdrCellLacSacId)
    ufdrCellLacSacSame.id should be(ufdrCellLacSacSameId)
    ufdrCellSecondLac.id should be(ufdrCellSecondLacId)
    ufdrCellSecondSac.id should be(ufdrCellSecondSacId)
  }

  it should "return tac/eci as identifier" in new WithEqualityCells {
    ufdrCellTacEci.id should be(ufdrCellTacEciId)
    ufdrCellTacEciSame.id should be(ufdrCellTacEciSameId)
    ufdrCellSecondTac.id should be(ufdrCellSecondTacId)
    ufdrCellSecondEci.id should be(ufdrCellSecondEciId)
  }

  it should "return tac/ci as identifier when no eci definned, but ci" in new WithEqualityCells {
    ufdrCellTacCi.id should be(ufdrCellTacCiId)
  }

  it should "return lac/-1 as identifier when no sac definned" in new WithEqualityCells {
    ufdrCellLac.id should be(ufdrCellLacId)
  }

  it should "return tac/-1 as identifier when no eci/ci definned" in new WithEqualityCells {
    ufdrCellTac.id should be(ufdrCellTacId)
  }

  it should "return -1/-1 as identifier when no lac/tac and no sac/eci/ci definned" in new WithEqualityCells {
    ufdrCellEmpty.id should be(ufdrCellEmptyId)
  }
}
