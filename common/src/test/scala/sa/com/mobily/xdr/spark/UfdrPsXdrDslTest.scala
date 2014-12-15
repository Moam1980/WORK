/*
 * TODO: License goes here!
 */

package sa.com.mobily.xdr.spark

import scala.reflect.io.File

import org.apache.spark.sql.catalyst.expressions.Row
import org.scalatest.{FlatSpec, ShouldMatchers}

import sa.com.mobily.event.Event
import sa.com.mobily.user.User
import sa.com.mobily.utils.LocalSparkSqlContext
import sa.com.mobily.xdr._

class UfdrPsXdrDslTest extends FlatSpec with ShouldMatchers with LocalSparkSqlContext {

  import UfdrPsXdrDsl._

  trait WithUfdrPsXdrsText {

    val ufdrPsXdrLine1 = "11692618241|1|1414184401|1414184401|1|1906|200912053883|420034103554735|357940040696441|" +
      "100.114.249.146|207.46.194.10|56194|80|WEB2|84.23.103.137|84.23.98.115||84.23.98.97|10.201.55.114|0|0FE7||" +
      "AF88||||504|40|1|1||0|c.bing.com|c.bing.com/c.gif?anx_uid=4894933205928070566&red3=msan_pd|" +
      "Mozilla/5.0(WindowsNT6.1;WOW64)AppleWebKit/537.36(KHTML-likeGecko)Chrome/38.0.2125.104Safari/537.36|" +
      "97|98||||420|03"
    val ufdrPsXdrLine2 = "560917079|420034122616618|1|866173010386736"
    val ufdrPsXdrLine3 = "11692618241|3|1414184401|1414184401|5|1906|200912053883|420034103554735|357940040696441|" +
      "100.114.249.146|207.46.194.10|56194|80|WEB2|84.23.103.137|84.23.98.115||84.23.98.97|10.201.55.114|0|0FE7||" +
      "AF88||||504|40|1|1||0|c.bing.com|c.bing.com/c.gif?anx_uid=4894933205928070566&red3=msan_pd|" +
      "Mozilla/5.0(WindowsNT6.1;WOW64)AppleWebKit/537.36(KHTML-likeGecko)Chrome/38.0.2125.104Safari/537.36|" +
      "97|98|1|1|1|420|03"
    val ufdrPsXdrs = sc.parallelize(List(ufdrPsXdrLine1, ufdrPsXdrLine2, ufdrPsXdrLine3))
  }

  trait WithUfdrPsXdrs {

    val cell1 = UfdrPSXdrCell(
      rat = Null,
      lac = "0FE7",
      rac = "",
      sac = "AF88",
      ci = "",
      tac = "",
      eci = "",
      mcc = "420",
      mnc = "03")
    val user1 = User(imei = "357940040696441", imsi = "420034103554735", msisdn = 200912053883L)
    val protocol1 = Protocol(category = P2P, id = 1906)
    val transferStats1 = TransferStats(
      l4UlThroughput = 504L,
      l4DwThroughput = 40L,
      l4UlPackets = 1,
      l4DwPackets = 1,
      dataTransUlDuration = 0L,
      dataTransDwDuration = 0L,
      ulLostRate = 0,
      dwLostRate = 0)
    val ufdrPsXdr1 = UfdrPsXdr(
      sid = 11692618241L,
      interfaceId = Gn,
      duration = Duration(beginTime = 1414184401L * 1000, endTime = 1414184401L * 1000),
      protocol = protocol1,
      user = user1,
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
      cell = cell1,
      transferStats = transferStats1,
      host = Some("c.bing.com"),
      firstUri = Some("c.bing.com/c.gif?anx_uid=4894933205928070566&red3=msan_pd"),
      userAgent = Some("Mozilla/5.0(WindowsNT6.1;WOW64)AppleWebKit/537.36(KHTML-likeGecko)" +
        "Chrome/38.0.2125.104Safari/537.36"),
      durationMsel = DurationMsel(beginTimeMsel = 97, endTimeMsel = 98),
      clickToContent = None)

    val transferStats2 = TransferStats(
      l4UlThroughput = 504L,
      l4DwThroughput = 40L,
      l4UlPackets = 1,
      l4DwPackets = 1,
      dataTransUlDuration = 0L,
      dataTransDwDuration = 0L,
      ulLostRate = 0,
      dwLostRate = 0)
    val ufdrPsXdr2 = UfdrPsXdr(
      sid = 11692618241L,
      interfaceId = Gn,
      duration = Duration(beginTime = 1414184401L * 1000, endTime = 1414184401L * 1000),
      protocol = protocol1,
      user = user1,
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
      cell = cell1,
      transferStats = transferStats2,
      host = Some("c.bing.com"),
      firstUri = Some("c.bing.com/c.gif?anx_uid=4894933205928070566&red3=msan_pd"),
      userAgent = Some("Mozilla/5.0(WindowsNT6.1;WOW64)AppleWebKit/537.36(KHTML-likeGecko)" +
        "Chrome/38.0.2125.104Safari/537.36"),
      durationMsel = DurationMsel(beginTimeMsel = 97, endTimeMsel = 98),
      clickToContent = None)

    val transferStats3 = TransferStats(
      l4UlThroughput = 504L,
      l4DwThroughput = 40L,
      l4UlPackets = 1,
      l4DwPackets = 1,
      dataTransUlDuration = 0L,
      dataTransDwDuration = 0L,
      ulLostRate = 0,
      dwLostRate = 0)
    val ufdrPsXdr3 = UfdrPsXdr(
      sid = 11692618241L,
      interfaceId = Gn,
      duration = Duration(beginTime = 1414184401L * 1000, endTime = 1414184401L * 1000),
      protocol = protocol1,
      user = user1,
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
      cell = cell1,
      transferStats = transferStats3,
      host = Some("c.bing.com"),
      firstUri = Some("c.bing.com/c.gif?anx_uid=4894933205928070566&red3=msan_pd"),
      userAgent = Some("Mozilla/5.0(WindowsNT6.1;WOW64)AppleWebKit/537.36(KHTML-likeGecko)" +
        "Chrome/38.0.2125.104Safari/537.36"),
      durationMsel = DurationMsel(beginTimeMsel = 97, endTimeMsel = 98),
      clickToContent = None)

    val ufdrPsXdrsAgg1 = UfdrPsXdrHierarchyAgg(
      hierarchy = UfdrPsXdrHierarchy(
        hourTime = "2014/10/25 00:00:00",
        cell = cell1,
        user = user1,
        protocol = protocol1),
      transferStats = TransferStats.aggregate(TransferStats.aggregate(transferStats1, transferStats2), transferStats3)
    )

    val ufdrPsXdrs = sc.parallelize(Array(ufdrPsXdr1, ufdrPsXdr2, ufdrPsXdr3))
    val ufdrPsXdrsAggs = sc.parallelize(Array(ufdrPsXdrsAgg1))
  }

  trait WithUfdrPsXdrsRows {

    val row = Row(11692618241L, Row(Gn.identifier), Row(1414184401L * 1000, 1414184401L * 1000),
      Row(Row(P2P.identifier), 1906), Row("357940040696441", "420034103554735", 200912053883L),
      Row("100.114.249.146", 56194), Row("207.46.194.10", 80), "WEB2", Row("84.23.103.137", ""),
      Row("84.23.98.115", "84.23.98.97"), "10.201.55.114",
      Row(Row(Null.identifier), "0FE7", "", "AF88", "", "", "", "420", "03"),
      Row(504L, 40L, 1, 1, 0, 0L, 0, 0),
      "c.bing.com", "c.bing.com/c.gif?anx_uid=4894933205928070566&red3=msan_pd",
      "Mozilla/5.0(WindowsNT6.1;WOW64)AppleWebKit/537.36(KHTML-likeGecko)Chrome/38.0.2125.104Safari/537.36",
      Row(97, 98), None)
    val row2 = Row(11692618241L, Row(Gn.identifier), Row(1414184401L * 1000, 1414184401L * 1000),
      Row(Row(P2P.identifier), 1906), Row("357940040696441", "420034103554735", 200912053883L),
      Row("100.114.249.146", 56194), Row("207.46.194.10", 80), "WEB2", Row("84.23.103.137", ""),
      Row("84.23.98.115", "84.23.98.97"), "10.201.55.114",
      Row(Row(Null.identifier), "0FE7", "", "AF88", "", "", "", "420", "03"),
      Row(504L, 40L, 1, 1, 0, 0L, 0, 0),
      "c.bing.com", "c.bing.com/c.gif?anx_uid=4894933205928070566&red3=msan_pd",
      "Mozilla/5.0(WindowsNT6.1;WOW64)AppleWebKit/537.36(KHTML-likeGecko)Chrome/38.0.2125.104Safari/537.36",
      Row(97, 98), None)
    val wrongRow = Row(11692618241L, Row(Gn.identifier), Row(1414184401L * 1000, 1414184401L * 1000),
      Row(Row(P2P.identifier), 1906), Row(200912053883L, "420034103554735", "357940040696441"),
      Row("100.114.249.146", 56194), Row("207.46.194.10", 80), "WEB2", Row("84.23.103.137", ""),
      Row("84.23.98.115", "84.23.98.97"), "10.201.55.114", Row(Null, "0FE7", "", "AF88", "", "", "", "420", "03"),
      Row(504L, 40L, 1, 1, 0, 0L, 0, 0), "c.bing.com",
      "c.bing.com/c.gif?anx_uid=4894933205928070566&red3=msan_pd",
      "Mozilla/5.0(WindowsNT6.1;WOW64)AppleWebKit/537.36(KHTML-likeGecko)Chrome/38.0.2125.104Safari/537.36",
      Row(97, 98), None)

    val rows = sc.parallelize(List(row, row2))
    val wrongRows = sc.parallelize(List(row, row2, wrongRow))
  }

  trait WithUfdrPsXdrEvents {

    val ufdrPsXdrLine1 = "11692618241|1|1414184401|1414184401|1|1906|200912053883|420034103554735|357940040696441|" +
      "100.114.249.146|207.46.194.10|56194|80|WEB2|84.23.103.137|84.23.98.115||84.23.98.97|10.201.55.114|0|0FE7||" +
      "AF88||||504|40|1|1||0|c.bing.com|c.bing.com/c.gif?anx_uid=4894933205928070566&red3=msan_pd|" +
      "Mozilla/5.0(WindowsNT6.1;WOW64)AppleWebKit/537.36(KHTML-likeGecko)Chrome/38.0.2125.104Safari/537.36|" +
      "97|98||||420|03"
    val ufdrPsXdrLine2 = "560917079|420034122616618|1|866173010386736"
    val ufdrPsXdrLine3 = "11692618241|3|1414184401|1414184401|5|1906|200912053883|420034103554735|357940040696441|" +
      "100.114.249.146|207.46.194.10|56194|80|WEB2|84.23.103.137|84.23.98.115||84.23.98.97|10.201.55.114|0|0FE7||" +
      "AF88||||504|40|1|1||0|c.bing.com|c.bing.com/c.gif?anx_uid=4894933205928070566&red3=msan_pd|" +
      "Mozilla/5.0(WindowsNT6.1;WOW64)AppleWebKit/537.36(KHTML-likeGecko)Chrome/38.0.2125.104Safari/537.36|" +
      "97|98|1|1|1|420|03"
    val ufdrPsXdrLine4 = "11692618241|3|1414184401|1414184401|1|1906|200912053883|420034103554735|357940040696441|" +
      "100.114.249.146|207.46.194.10|56194|80|WEB2|84.23.103.137|84.23.98.115||84.23.98.97|10.201.55.114|0|||" +
      "||0F0F|F0F0|504|40|1|1||0|c.bing.com|c.bing.com/c.gif?anx_uid=4894933205928070566&red3=msan_pd|" +
      "Mozilla/5.0(WindowsNT6.1;WOW64)AppleWebKit/537.36(KHTML-likeGecko)Chrome/38.0.2125.104Safari/537.36|" +
      "97|98|1|1|1|420|03"
    val ufdrPsXdrLine5 = "11692618241|3|1414184401|1414184401|1|1906|200912053883||357940040696441|" +
      "100.114.249.146|207.46.194.10|56194|80|WEB2|84.23.103.137|84.23.98.115||84.23.98.97|10.201.55.114|0|||" +
      "||0F0F|F0F0|504|40|1|1||0|c.bing.com|c.bing.com/c.gif?anx_uid=4894933205928070566&red3=msan_pd|" +
      "Mozilla/5.0(WindowsNT6.1;WOW64)AppleWebKit/537.36(KHTML-likeGecko)Chrome/38.0.2125.104Safari/537.36|" +
      "97|98|1|1|1|420|03"
    val ufdrPsXdrLine6 = "11692618241|3|1414184401|1414184401|1|1906||420034103554735|357940040696441|" +
      "100.114.249.146|207.46.194.10|56194|80|WEB2|84.23.103.137|84.23.98.115||84.23.98.97|10.201.55.114|0|||" +
      "||0F0F|F0F0|504|40|1|1||0|c.bing.com|c.bing.com/c.gif?anx_uid=4894933205928070566&red3=msan_pd|" +
      "Mozilla/5.0(WindowsNT6.1;WOW64)AppleWebKit/537.36(KHTML-likeGecko)Chrome/38.0.2125.104Safari/537.36|" +
      "97|98|1|1|1|420|03"
    val ufdrPsXdrLine7 = "11692618241|3|1414184401|1414184401|1|1906|||357940040696441|" +
      "100.114.249.146|207.46.194.10|56194|80|WEB2|84.23.103.137|84.23.98.115||84.23.98.97|10.201.55.114|0|||" +
      "||0F0F|F0F0|504|40|1|1||0|c.bing.com|c.bing.com/c.gif?anx_uid=4894933205928070566&red3=msan_pd|" +
      "Mozilla/5.0(WindowsNT6.1;WOW64)AppleWebKit/537.36(KHTML-likeGecko)Chrome/38.0.2125.104Safari/537.36|" +
      "97|98|1|1|1|420|03"
    val ufdrPsXdrLine8 = "11692618241|3|1414184401|1414184401|1|1906||420034103554735||" +
      "100.114.249.146|207.46.194.10|56194|80|WEB2|84.23.103.137|84.23.98.115||84.23.98.97|10.201.55.114|0|||" +
      "||0F0F|F0F0|504|40|1|1||0|c.bing.com|c.bing.com/c.gif?anx_uid=4894933205928070566&red3=msan_pd|" +
      "Mozilla/5.0(WindowsNT6.1;WOW64)AppleWebKit/537.36(KHTML-likeGecko)Chrome/38.0.2125.104Safari/537.36|" +
      "97|98|1|1|1|420|03"
    val ufdrPsXdrLine9 = "11692618241|3|1414184401|1414184401|1|1906|200912053883|||" +
      "100.114.249.146|207.46.194.10|56194|80|WEB2|84.23.103.137|84.23.98.115||84.23.98.97|10.201.55.114|0|||" +
      "||0F0F|F0F0|504|40|1|1||0|c.bing.com|c.bing.com/c.gif?anx_uid=4894933205928070566&red3=msan_pd|" +
      "Mozilla/5.0(WindowsNT6.1;WOW64)AppleWebKit/537.36(KHTML-likeGecko)Chrome/38.0.2125.104Safari/537.36|" +
      "97|98|1|1|1|420|03"
    val ufdrPsXdrLine10 = "11692618241|1||1414184401|1|1906|200912053883|420034103554735|357940040696441|" +
      "100.114.249.146|207.46.194.10|56194|80|WEB2|84.23.103.137|84.23.98.115||84.23.98.97|10.201.55.114|0|0FE7||" +
      "AF88||||504|40|1|1||0|c.bing.com|c.bing.com/c.gif?anx_uid=4894933205928070566&red3=msan_pd|" +
      "Mozilla/5.0(WindowsNT6.1;WOW64)AppleWebKit/537.36(KHTML-likeGecko)Chrome/38.0.2125.104Safari/537.36|" +
      "97|98||||420|03"
    val ufdrPsXdrLine11 = "11692618241|1|1414184401||1|1906|200912053883|420034103554735|357940040696441|" +
      "100.114.249.146|207.46.194.10|56194|80|WEB2|84.23.103.137|84.23.98.115||84.23.98.97|10.201.55.114|0|0FE7||" +
      "AF88||||504|40|1|1||0|c.bing.com|c.bing.com/c.gif?anx_uid=4894933205928070566&red3=msan_pd|" +
      "Mozilla/5.0(WindowsNT6.1;WOW64)AppleWebKit/537.36(KHTML-likeGecko)Chrome/38.0.2125.104Safari/537.36|" +
      "97|98||||420|03"
    val ufdrPsXdrs = sc.parallelize(
      List(ufdrPsXdrLine1, ufdrPsXdrLine2, ufdrPsXdrLine3, ufdrPsXdrLine4, ufdrPsXdrLine5, ufdrPsXdrLine6,
        ufdrPsXdrLine7, ufdrPsXdrLine8, ufdrPsXdrLine9, ufdrPsXdrLine10, ufdrPsXdrLine11))

    val event1 = Event(
      user = User("357940040696441", "420034103554735", 200912053883L),
      beginTime = 1414184401L * 1000,
      endTime = 1414184401L * 1000,
      lacTac = 4071,
      cellId = 44936,
      eventType = "1.1906",
      subsequentLacTac = None,
      subsequentCellId = None)

    val event3 = event1.copy(eventType = "5.1906")
    val event4 = event1.copy(lacTac = 3855, cellId = 61680)
    val event5 = event4.copy(user = User("", "420034103554735", 200912053883L))
    val event6 = event4.copy(user = User("357940040696441", "420034103554735", 0L))
    val event7 = event4.copy(user = User("357940040696441", "", 0L))
    val event8 = event4.copy(user = User("", "420034103554735", 0L))
    val event9 = event4.copy(user = User("", "", 200912053883L))
    val events = sc.parallelize(Array(event1, event3, event4, event5, event6, event7, event8, event9))
  }

  "UfdrPsXdrDsl" should "get correctly parsed ufdrPsXdrs" in new WithUfdrPsXdrsText {
    ufdrPsXdrs.toUfdrPsXdr.count should be (2)
  }

  it should "get errors when parsing ufdrPsXdrs" in new WithUfdrPsXdrsText {
    ufdrPsXdrs.toUfdrPsXdrErrors.count should be (1)
  }

  it should "get both correctly and wrongly parsed PS ufdrPsXdrs" in new WithUfdrPsXdrsText {
    ufdrPsXdrs.toParsedUfdrPsXdr.count should be (3)
  }

  it should "get correctly parsed rows" in new WithUfdrPsXdrsRows {
    rows.toUfdrPsXdr.count should be (2)
  }

  it should "throw an exception when rows have errors" in new WithUfdrPsXdrsRows {
    an[Exception] should be thrownBy wrongRows.toUfdrPsXdr.count
  }

  it should "save ufdrPsXdrs in parquet" in new WithUfdrPsXdrs {
    val path = File.makeTemp().name
    ufdrPsXdrs.saveAsParquetFile(path)
    sqc.parquetFile(path).toUfdrPsXdr.collect.sameElements(ufdrPsXdrs.collect) should be (true)
    File(path).deleteRecursively
  }

  it should "aggregate ufdrPsXdrs by hour, cell, user and protocol" in new WithUfdrPsXdrs {
    ufdrPsXdrs.perHourCellUserAndProtocol.collect should be (ufdrPsXdrsAggs.collect)
  }

  it should "parse RDD[UfdrPsXdrs] to RDD[Event]" in new WithUfdrPsXdrEvents {
    ufdrPsXdrs.toUfdrPsXdr.toEvent.collect should be (events.collect)
  }

  it should "save bad formatted records in a CSV file" in new WithUfdrPsXdrsText {
    val path = File.makeTemp().name
    ufdrPsXdrs.saveErrors(path)
    sc.textFile(path).count should be (1)
    File(path).deleteRecursively
  }
}
