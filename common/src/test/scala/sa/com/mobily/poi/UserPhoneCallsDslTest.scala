/*
 * TODO: License goes here!
 */

package sa.com.mobily.poi.spark

import com.github.nscala_time.time.Imports._
import org.apache.spark.mllib.linalg.Vectors
import org.scalatest._

import sa.com.mobily.poi.UserPhoneCalls
import sa.com.mobily.utils.EdmCoreUtils
import sa.com.mobily.utils.LocalSparkContext

class UserPhoneCallsDslTest extends  FlatSpec with ShouldMatchers with LocalSparkContext{

  import UserPhoneCallsDsl._

  trait WithPhoneCallsDslText {

    val phoneCall1 = "'0500001413','20140824',2541,'1','01, 02'"
    val phoneCall2 = "'0500001413','20140824',2806,'1','13, 19, 20'"
    val phoneCall3 = "'XXXXXX','20140824',4576,'1','19'"
    val phoneCall4 = "'0500001413','20140825',6051,'1','10, 11'"
    val phoneCall5 = "'0500001413','20140825',5346,'1','01, 02, 03, 04, 05, 07, 08, 09'"

    val phoneCalls = sc.parallelize(List(phoneCall1, phoneCall2, phoneCall3, phoneCall4, phoneCall5))
  }

  trait WithPhoneCalls{

    val phoneCall1 = UserPhoneCalls(1L,DateTimeFormat.forPattern("yyyymmdd").withZone(EdmCoreUtils.TimeZoneSaudiArabia)
      .parseDateTime("20140824"),"2541", 1, Seq(1, 2))
    val phoneCall2 = UserPhoneCalls(1L,DateTimeFormat.forPattern("yyyymmdd").withZone(EdmCoreUtils.TimeZoneSaudiArabia)
      .parseDateTime("20140824"),"2555", 1, Seq(1, 2))
    val phoneCall3 = UserPhoneCalls(2L,DateTimeFormat.forPattern("yyyymmdd").withZone(EdmCoreUtils.TimeZoneSaudiArabia)
      .parseDateTime("20140824"),"2566", 1, Seq(1, 2))
    val phoneCall4 = UserPhoneCalls(2L,DateTimeFormat.forPattern("yyyymmdd").withZone(EdmCoreUtils.TimeZoneSaudiArabia)
      .parseDateTime("20140824"),"2566", 1, Seq(1, 2, 3))
    val phoneCall5 = UserPhoneCalls(3L,DateTimeFormat.forPattern("yyyymmdd").withZone(EdmCoreUtils.TimeZoneSaudiArabia)
      .parseDateTime("20140824"),"2577", 1, Seq(1, 2))

    val phoneCalls = sc.parallelize(List(phoneCall1, phoneCall2, phoneCall3, phoneCall4, phoneCall5))
  }

  trait WithWeekPhoneCalls{

    val phoneCall1 = UserPhoneCalls(1L,DateTimeFormat.forPattern("yyyyMMdd").withZone(EdmCoreUtils.TimeZoneSaudiArabia)
      .parseDateTime("20140824"),"2541", 1, Seq(0, 1, 2))
    val phoneCall2 = phoneCall1.copy(timestamp = DateTimeFormat.forPattern("yyyyMMdd").
      withZone(EdmCoreUtils.TimeZoneSaudiArabia).parseDateTime("20140818"),callHours = Seq(0, 23))
    val phoneCall3 = phoneCall1.copy(timestamp = DateTimeFormat.forPattern("yyyyMMdd").
      withZone(EdmCoreUtils.TimeZoneSaudiArabia).parseDateTime("20140819"),callHours = Seq(1, 23))
    val phoneCall4 = UserPhoneCalls(2L,DateTimeFormat.forPattern("yyyyMMdd").withZone(EdmCoreUtils.TimeZoneSaudiArabia)
      .parseDateTime("20140825"),"2566", 1, Seq(1, 2, 3))

    val phoneCalls = sc.parallelize(List(phoneCall1, phoneCall2, phoneCall3, phoneCall4))
    val vectorResult = Vectors.dense(1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0,
      0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0,
      0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0,
      0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0,
      0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0,
      0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0,
      0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 1.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0,
      0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0)
  }

  "PhoneCallsDsl" should "get correctly parsed phoneCalls" in new WithPhoneCallsDslText {
    phoneCalls.toParsedPhoneCalls.count() should be (5)
  }

  it should "get errors when parsing phoneCalls" in new WithPhoneCallsDslText {
    phoneCalls.toPhoneCallsErrors.count should be (1)
  }

  it should "get both correctly and wrongly parsed phoneCalls" in new WithPhoneCallsDslText {
    phoneCalls.toPhoneCalls.count should be (4)
  }

  it should "calculate the vector to home clustering correctly" in new WithWeekPhoneCalls {
    val homes = phoneCalls.perUserAndSiteId.collect
    homes.length should be (2)
    homes.tail.head._2 should be (vectorResult)
  }
}
