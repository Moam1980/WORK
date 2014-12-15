/*
 * TODO: License goes here!
 */

package sa.com.mobily.xdr.spark

import scala.reflect.io.File

import org.scalatest.{FlatSpec, ShouldMatchers}

import sa.com.mobily.event.Event
import sa.com.mobily.user.User
import sa.com.mobily.utils.LocalSparkSqlContext

class IuCsXdrDslTest extends FlatSpec with ShouldMatchers with LocalSparkSqlContext {

  import IuCsXdrDsl._

  trait WithIuCsXdrText {

    val iuCsXdrLine1 = "2,04,0149b9851507,0149b9851a93,0,0,0,0,9dd,22c,4939008,13173274,58c,131,142,_,_,_,_,_,_,_,_," +
      "_,_,d4b,_,d4b,_,_,_,_,83,_,_,_,_,_,_,_,_,420,03,0,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_," +
      "420032275422214,_,8636190157279614,a65ca98e,b25cb3dc,_,_,_,_,_,_,_,_,_,ef9,d4b,82eb,_,c00c0086,_,_,_,_,_,0,_," +
      "166,_,_,_,51a00200,0,82eb,_,1,_,_,_,_,_"
    val iuCsXdrLine2 = "2,04,0149b9851369,0149b9851a97,0,0,0,0,240,22c,8765446,5002776,72e,131,142,_,_,_,_,_,_,_,_," +
      "_,_,d00,_,d00,_,_,_,_,83,_,_,_,_,_,_,_,_,420,03,0,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_," +
      "420032186210267,_,3542180683342601,b05c6c85,b75c75e7,_,_,_,_,_,_,_,_,_,e11,d00,d1f,_,c00c0086,_,_,_,_,_,0,_," +
      "14d,_,_,_,51a00200,0,d1f,_,1,_,_,_,_,_"
    val iuCsXdrLine3 = "2,04,0149b9851369,0149b9851a97,0,0,0,0,240,22c,8765446,5002776,72e,131,142,_,_,_,_,_,_,_,_,_,_"
    val iuCsXdrEvents = sc.parallelize(Array(iuCsXdrLine1, iuCsXdrLine2, iuCsXdrLine3))
  }

  trait WithIuCsXdrAndEventText {

    val iuCsXdrLine1 = "2,04,0149b9851507,0149b9851a93,0,0,0,0,9dd,22c,4939008,13173274,58c,131,142,_,_,_,_,_,_,_,_," +
      "_,_,d4b,_,d4b,_,_,_,_,83,_,_,_,_,_,_,_,_,420,03,0,_,_,_,_,_,_,_,0,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_," +
      "420032275422214,8636190157279614,8636190157279614,a65ca98e,b25cb3dc,_,666666666,_,_,_,_,_,_,_,ef9,d4b,82eb,_," +
      "c00c0086,_,_,_,_,_,0,_," +
      "166,_,_,_,51a00200,0,82eb,_,1,_,_,_,_,_"
    val iuCsXdrLine2 = "2,04,0149b9851369,0149b9851a97,0,0,0,0,240,22c,8765446,5002776,72e,131,142,_,_,_,_,_,_,_,_," +
      "_,_,d00,_,d00,_,_,_,_,83,_,_,_,_,_,_,_,_,420,03,0,_,_,_,_,_,_,_,1,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_," +
      "420032186210267,3542180683342601,3542180683342601,b05c6c85,b75c75e7,_,777777777,_,_,_,_,_,_,_,e11,d00,d1f,_," +
      "c00c0086,_,_,_,_,_,0,_,14d,_,_,_,51a00200,0,d1f,_,1,_,_,_,_,_"
    val iuCsXdrLine3 = "2,04,0149b9851369,0149b9851a97,0,0,0,0,240,22c,8765446,5002776,72e,131,142,_,_,_,_,_,_,_,_," +
      "_,_,d00,_,d00,_,_,_,_,83,_,_,_,_,_,_,_,_,420,03,0,_,_,_,_,_,_,_,1,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_," +
      "420032186210267,3542180683342601,3542180683342601,b05c6c85,b75c75e7,_,_,_,_,_,_,_,_,_,e11,d00,d1f,_," +
      "c00c0086,_,_,_,_,_,0,_,14d,_,_,_,51a00200,0,d1f,_,1,_,_,_,_,_"
    val iuCsXdrEvents = sc.parallelize(Array(iuCsXdrLine1, iuCsXdrLine2, iuCsXdrLine3))
    val event1 = Event(
      User("8636190157279614", "420032275422214", 666666666),
      1416156747015L,
      1416156748435L,
      3403,
      33515,
      "0",
      None,
      None,
      None,
      None,
      None)
    val event2 = Event(
      User("3542180683342601", "420032186210267", 777777777),
      1416156746601L,
      1416156748439L,
      3328,
      3359,
      "1",
      None,
      None,
      None,
      None,
      None)
    val events = sc.parallelize(Array(event1, event2))
  }

  "IuCsXdrDsl" should "get correctly parsed IuCS events" in new WithIuCsXdrText {
    iuCsXdrEvents.toIuCsXdr.count should be (2)
  }

  it should "get errors when parsing IuCS events" in new WithIuCsXdrText {
    iuCsXdrEvents.toIuCsXdrErrors.count should be (1)
  }

  it should "get both correctly and wrongly parsed IuCS events" in new WithIuCsXdrText {
    iuCsXdrEvents.toParsedIuCsXdr.count should be (3)
  }

  it should "save and read IuCS events in parquet" in new WithIuCsXdrText {
    val path = File.makeTemp().name
    val parsedEvents = iuCsXdrEvents.toIuCsXdr
    parsedEvents.saveAsParquetFile(path)
    sqc.parquetFile(path).toIuCsXdr.collect should be (parsedEvents.collect)
    File(path).deleteRecursively
  }

  it should "get correctly parsed IuCS to Events" in new WithIuCsXdrAndEventText {
    iuCsXdrEvents.toIuCsXdr.toEvent.count should be (2)
  }

  it should "save bad formatted records in a CSV file" in new WithIuCsXdrText {
    val path = File.makeTemp().name
    iuCsXdrEvents.saveErrors(path)
    sc.textFile(path).count should be (1)
    File(path).deleteRecursively
  }

  it should "parse RDD[IuCsXdr] to RDD[Event]" in new WithIuCsXdrAndEventText {
    val iuCsEvents = iuCsXdrEvents.toIuCsXdr
    iuCsEvents.toEvent.collect should be (events.collect)
  }
}
