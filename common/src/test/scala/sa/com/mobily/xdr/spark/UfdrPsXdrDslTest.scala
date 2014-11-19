/*
 * TODO: License goes here!
 */

package sa.com.mobily.xdr.spark

import scala.reflect.io.File

import org.apache.spark.sql.catalyst.expressions.Row
import org.scalatest.{FlatSpec, ShouldMatchers}

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

    val ufdrPsXdr1 = UfdrPsXdr(
      sid = 11692618241L,
      interfaceId = Gn,
      duration = Duration(beginTime = 1414184401L*1000, endTime = 1414184401L*1000),
      protocol = Protocol(
        category = P2P,
        id = 1906),
      user = User(
        imei = "357940040696441",
        imsi = "420034103554735",
        msisdn = 200912053883L),
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
      cell = UfdrPSXdrCell(
        rat = Null,
        lac = "0FE7",
        rac = "",
        sac = "AF88",
        ci = "",
        tac = "",
        eci = "",
        mcc = "420",
        mnc = "03"),
      transferStats = TransferStats(
        l4UlThroughput = 504L,
        l4DwThroughput = 40L,
        l4UlPackets = 1,
        l4DwPackets = 1,
        dataTransUlDuration = 0L,
        dataTransDwDuration = 0L,
        ulLostRate = 0,
        dwLostRate = 0),
      host = Some("c.bing.com"),
      firstUri = Some("c.bing.com/c.gif?anx_uid=4894933205928070566&red3=msan_pd"),
      userAgent = Some("Mozilla/5.0(WindowsNT6.1;WOW64)AppleWebKit/537.36(KHTML-likeGecko)" +
        "Chrome/38.0.2125.104Safari/537.36"),
      durationMsel = DurationMsel(beginTimeMsel = 97, endTimeMsel = 98),
      clickToContent = None)

    val ufdrPsXdr2 = UfdrPsXdr(
      sid = 11692618241L,
      interfaceId = Gn,
      duration = Duration(beginTime = 1414184401L*1000, endTime = 1414184401L*1000),
      protocol = Protocol(
        category = P2P,
        id = 1906),
      user = User(
        imei = "357940040696441",
        imsi = "420034103554735",
        msisdn = 200912053883L),
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
      cell = UfdrPSXdrCell(
        rat = Null,
        lac = "0FE7",
        rac = "",
        sac = "AF88",
        ci = "",
        tac = "",
        eci = "",
        mcc = "420",
        mnc = "03"),
      transferStats = TransferStats(
        l4UlThroughput = 504L,
        l4DwThroughput = 40L,
        l4UlPackets = 1,
        l4DwPackets = 1,
        dataTransUlDuration = 0L,
        dataTransDwDuration = 0L,
        ulLostRate = 0,
        dwLostRate = 0),
      host = Some("c.bing.com"),
      firstUri = Some("c.bing.com/c.gif?anx_uid=4894933205928070566&red3=msan_pd"),
      userAgent = Some("Mozilla/5.0(WindowsNT6.1;WOW64)AppleWebKit/537.36(KHTML-likeGecko)" +
        "Chrome/38.0.2125.104Safari/537.36"),
      durationMsel = DurationMsel(beginTimeMsel = 97, endTimeMsel = 98),
      clickToContent = None)

    val ufdrPsXdr3 = UfdrPsXdr(
      sid = 11692618241L,
      interfaceId = Gn,
      duration = Duration(beginTime = 1414184401L*1000, endTime = 1414184401L*1000),
      protocol = Protocol(
        category = P2P,
        id = 1906),
      user = User(
        imei = "357940040696441",
        imsi = "420034103554735",
        msisdn = 200912053883L),
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
      cell = UfdrPSXdrCell(
        rat = Null,
        lac = "0FE7",
        rac = "",
        sac = "AF88",
        ci = "",
        tac = "",
        eci = "",
        mcc = "420",
        mnc = "03"),
      transferStats = TransferStats(
        l4UlThroughput = 504L,
        l4DwThroughput = 40L,
        l4UlPackets = 1,
        l4DwPackets = 1,
        dataTransUlDuration = 0L,
        dataTransDwDuration = 0L,
        ulLostRate = 0,
        dwLostRate = 0),
      host = Some("c.bing.com"),
      firstUri = Some("c.bing.com/c.gif?anx_uid=4894933205928070566&red3=msan_pd"),
      userAgent = Some("Mozilla/5.0(WindowsNT6.1;WOW64)AppleWebKit/537.36(KHTML-likeGecko)" +
        "Chrome/38.0.2125.104Safari/537.36"),
      durationMsel = DurationMsel(beginTimeMsel = 97, endTimeMsel = 98),
      clickToContent = None)

    val ufdrPsXdrs = sc.parallelize(Array(ufdrPsXdr1, ufdrPsXdr2, ufdrPsXdr3))
  }

  trait WithUfdrPsXdrsRows {

    val row = Row(11692618241L, Row(Gn.identifier), Row(1414184401L*1000, 1414184401L*1000),
      Row(Row(P2P.identifier), 1906), Row("357940040696441", "420034103554735", 200912053883L),
      Row("100.114.249.146", 56194), Row("207.46.194.10", 80), "WEB2", Row("84.23.103.137", ""),
      Row("84.23.98.115", "84.23.98.97"), "10.201.55.114",
      Row(Row(Null.identifier), "0FE7", "", "AF88", "", "", "", "420", "03"),
      Row(504L, 40L, 1, 1, 0, 0L, 0, 0),
      "c.bing.com", "c.bing.com/c.gif?anx_uid=4894933205928070566&red3=msan_pd",
      "Mozilla/5.0(WindowsNT6.1;WOW64)AppleWebKit/537.36(KHTML-likeGecko)Chrome/38.0.2125.104Safari/537.36",
      Row(97, 98), None)
    val row2 = Row(11692618241L, Row(Gn.identifier), Row(1414184401L*1000, 1414184401L*1000),
      Row(Row(P2P.identifier), 1906), Row("357940040696441", "420034103554735", 200912053883L),
      Row("100.114.249.146", 56194), Row("207.46.194.10", 80), "WEB2", Row("84.23.103.137", ""),
      Row("84.23.98.115", "84.23.98.97"), "10.201.55.114",
      Row(Row(Null.identifier), "0FE7", "", "AF88", "", "", "", "420", "03"),
      Row(504L, 40L, 1, 1, 0, 0L, 0, 0),
      "c.bing.com", "c.bing.com/c.gif?anx_uid=4894933205928070566&red3=msan_pd",
      "Mozilla/5.0(WindowsNT6.1;WOW64)AppleWebKit/537.36(KHTML-likeGecko)Chrome/38.0.2125.104Safari/537.36",
      Row(97, 98), None)
    val wrongRow = Row(11692618241L, Row(Gn.identifier), Row(1414184401L*1000, 1414184401L*1000),
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
}
