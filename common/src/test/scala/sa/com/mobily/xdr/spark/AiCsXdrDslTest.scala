/*
 * TODO: License goes here!
 */

package sa.com.mobily.xdr.spark

import scala.reflect.io.File

import org.scalatest.{FlatSpec, ShouldMatchers}

import sa.com.mobily.event.{CsAInterfaceSource, Event}
import sa.com.mobily.user.User
import sa.com.mobily.utils.{LocalSparkSqlContext}

class AiCsXdrDslTest extends FlatSpec with ShouldMatchers with LocalSparkSqlContext {

  import AiCsXdrDsl._

  trait WithAiCsEvents {

    val aiCsXdrLine1 = "0,0149b9829192,0149b982d8e6,000392,000146,0,201097345297,01,_,_,1751,1,8,21,_," +
      "420034104770740,61c5f3e5,0839,517d,_,_,_,00004330,_,_,00004754,00000260,00000702,_,_,0,_,9,_,_,_," +
      "10,_,_,0839,517d,_,_,_,_,0839,_,_,10,_,_,03,61c5f3e5,3523870633105423,00,_,0,_,_,0,0,254,_,_,_,_,1,_,0" +
      ",_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,d8000c00,80000,230e0000,0,0,16,_,_,_,_,00,0839,_,_,_,_,424e,_,0," +
      "ba,11,_,1,0,60"
    val aiCsXdrLine2 = "0149b982da5b,000251,000146,_,_,_,_,_,_,_,1"
    val aiCsXdrLine3 = "4,0149b982d580,0149b982da5b,000251,000146,_,_,_,_,_,_,_,1,_,_,420032370264779,b0c5b874," +
      "0a3e,e882,_,_,_,_,_,_,000004db,_,_,_,_,0,_,9,_,_,_,_,_,_,0a3e,e882,_,_,420,03,0887,_,_,_,_,_,_," +
      "3547240635999912,b0c5b874,00,0,_,_,_,0,0,254,_,_,_,_,_,_,0,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_," +
      "18000c00,56000000,0,0,0,_,_,_,_,_,00,0a3e,_,_,_,_,_,3,0,_,_,_,1,_,60"
    val aiCsXdrs = sc.parallelize(List(aiCsXdrLine1, aiCsXdrLine2, aiCsXdrLine3))
  }

  trait WithAiCsEventsToParse {

    val aiCsXdrLine1 = "0,0149b9829192,0149b982d8e6,000392,000146,0,201097345297,01,_,_,1751,1,8,21,42104770740," +
      "420034104770740,61c5f3e5,0839,517d,_,_,_,00004330,_,_,00004754,00000260,00000702,_,_,0,_,9,_,_,420034770740," +
      "10,_,_,0839,517d,_,_,_,_,0839,_,_,10,_,_,03,61c5f3e5,3523870633105423,00,_,0,_,_,0,0,254,_,_,_,_,1,_,0" +
      ",_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,d8000c00,80000,230e0000,0,0,16,_,_,_,_,00,0839,_,1a,_,_,424e,_,0," +
      "ba,11,3523870,1,0,60"
    val aiCsXdrLine2 = "4,0149b982d580,0149b982da5b,000251,000146,1,_,_,_,_,_,_,1,_,4202370264779,420032370264779," +
      "b0c5b874,0a3e,e882,_,_,_,_,_,_,000004db,_,_,_,_,0,_,9,_,_,_,_,_,_,0a3e,e882,_,_,420,03,0887,_,_,_,_,_,2131231," +
      "3547240635999912,b0c5b874,00,0,_,_,_,0,0,254,_,_,_,_,_,_,0,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,18000c00," +
      "56000000,0,0,0,_,_,_,_,_,00,0a3e,_,1a,_,_,_,3,0,_,_,3523871,1,_,60"
    val aiCsXdrLine3 = "4,0149b982d580,0149b982da5b,000251,000146,1,_,_,_,_,_,_,1,_,4202370264779,_,b0c5b874,0a3e," +
      "e882,_,_,_,_,_,_,000004db,_,_,_,_,0,_,9,_,_,_,_,_,_,0a3e,e882,_,_,420,03,0887,_,_,_,_,_,2131231," +
      "3547240635999912,b0c5b874,00,0,_,_,_,0,0,254,_,_,_,_,_,_,0,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,18000c00," +
      "56000000,0,0,0,_,_,_,_,_,00,0a3e,_,1a,_,_,_,3,0,_,_,_,1,_,60"
    val aiCsXdrLine4 = "4,0149b982d580,0149b982da5b,000251,000146,1,_,_,_,_,_,_,1,_,_,420032370264779,b0c5b874,0a3e," +
      "e882,_,_,_,_,_,_,000004db,_,_,_,_,0,_,9,_,_,_,_,_,_,0a3e,e882,_,_,420,03,0887,_,_,_,_,_,2131231," +
      "3547240635999912,b0c5b874,00,0,_,_,_,0,0,254,_,_,_,_,_,_,0,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,18000c00," +
      "56000000,0,0,0,_,_,_,_,_,00,0a3e,_,1a,_,_,_,3,0,_,_,_,1,_,60"
    val aiCsXdrLine5 = "4,0149b982d580,0149b982da5b,000251,000146,1,_,_,_,_,_,_,1,_,_,_,b0c5b874,0a3e,e882,_,_,_,_,_," +
      "_,000004db,_,_,_,_,0,_,9,_,_,_,_,_,_,0a3e,e882,_,_,420,03,0887,_,_,_,_,_,2131231,3547240635999912,b0c5b874," +
      "00,0,_,_,_,0,0,254,_,_,_,_,_,_,0,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,18000c00,56000000,0,0,0,_,_,_,_,_," +
      "00,0a3e,_,1a,_,_,_,3,0,_,_,3523871,1,_,60"
    val aiCsXdrLine6 = "0,0149b9829192,0149b982d8e6,000392,000146,0,201097345297,01,_,_,1751,1,8,21,_,_,61c5f3e5," +
      "0839,517d,_,_,_,00004330,_,_,00004754,00000260,00000702,_,_,0,_,9,_,_,420034770740,10,_,_,0839,517d,_,_,_,_," +
      "0839,_,_,10,_,_,03,61c5f3e5,3523870633105423,00,_,0,_,_,0,0,254,_,_,_,_,1,_,0,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_," +
      "_,_,_,_,_,d8000c00,80000,230e0000,0,0,16,_,_,_,_,00,0839,_,1a,_,_,424e,_,0,ba,11,_,1,0,60"
    val aiCsXdrs = sc.parallelize(Array(
      aiCsXdrLine1,
      aiCsXdrLine2,
      aiCsXdrLine3,
      aiCsXdrLine4,
      aiCsXdrLine5,
      aiCsXdrLine6))
    val event1 = Event(
      User("42104770740", "420034104770740", 3523870),
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
    val event2 = Event(
      User("4202370264779", "420032370264779", 3523871),
      1416156599680L,
      1416156600923L,
      2622,
      59522,
      CsAInterfaceSource,
      Some("4"),
      None,
      None,
      None,
      None,
      None)
    val event3 = event2.copy(user = User("4202370264779", "", 0L))
    val event4 = event2.copy(user = User("", "420032370264779", 0L))
    val event5 = event2.copy(user = User("", "", 3523871))
    val eventsParsed = sc.parallelize(Array(event1, event2, event3, event4, event5))
  }

  trait WithSanity extends WithAiCsEventsToParse {

    val metrics = List(("msisdn", 3), ("imei", 3), ("imsi", 3), ("total", 6))
  }

  "AiCsXdrDsl" should "get correctly parsed AiCS events" in new WithAiCsEvents {
    aiCsXdrs.toAiCsXdr.count should be (2)
  }

  it should "get errors when parsing AiCsXdrs" in new WithAiCsEvents {
    aiCsXdrs.toAiCsXdrErrors.count should be (1)
  }

  it should "get both correctly and wrongly parsed AiCsXdrs" in new WithAiCsEvents {
    aiCsXdrs.toParsedAiCsXdr.count should be (3)
  }

  it should "save events in parquet" in new WithAiCsEvents {
    val path = File.makeTemp().name
    val events = aiCsXdrs.toAiCsXdr
    events.saveAsParquetFile(path)
    sqc.parquetFile(path).toAiCsXdr.collect.sameElements(events.collect) should be (true)
    File(path).deleteRecursively
  }

  it should "save bad formatted records in a CSV file" in new WithAiCsEvents {
    val path = File.makeTemp().name
    aiCsXdrs.saveErrors(path)
    sc.textFile(path).count should be (1)
    File(path).deleteRecursively
  }

  it should "parse RDD[AiCsXdr] to RDD[Event]" in new WithAiCsEventsToParse {
    aiCsXdrs.toAiCsXdr.toEvent.collect should be (eventsParsed.collect)
  }

  it should "take sanity metrics" in new WithSanity {
    aiCsXdrs.toAiCsXdr.sanity.collect.sameElements(metrics) should be (true)
  }
}
