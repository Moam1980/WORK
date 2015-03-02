/*
 * TODO: License goes here!
 */

package sa.com.mobily.ia.spark

import scala.reflect.io.File

import org.apache.spark.sql.catalyst.expressions.Row
import org.scalatest._

import sa.com.mobily.ia.SubscriberIa
import sa.com.mobily.utils.LocalSparkSqlContext

class SubscriberIaDslTest extends FlatSpec with ShouldMatchers with LocalSparkSqlContext {

  import SubscriberIaDsl._

  trait WithSubscriberIaText {

    val subscriber1 = "\"18711223\"|\"966560000000\"|\"1\"|\"19810723\"|\"\"|\"966\"|\"\"|\"2\"|\"33\"|\"\"|" +
      "\"\"|\"\"|\"\"|\"\"|\"\"|\"7\""
    val subscriber2 = "\"12345678\"|\"966561111111\"|\"44\"|\"19810723\"|\"\"|\"966\"|\"\"|\"4\"|\"53\"|\"\"|" +
      "\"\"|\"\"|\"\"|\"\"|\"\"|\"7\""
    val subscriber3 = "\"18711223\"|\"WrongNumber\"|\"1\"|\"19810723\"|\"\"|\"966\"|\"\"|\"2\"|\"33\"|\"\"|" +
      "\"\"|\"\"|\"\"|\"\"|\"\"|\"7\""

    val subscriber = sc.parallelize(List(subscriber1, subscriber2,
      subscriber3))
  }

  trait WithSubscriberIaRows {

    val row = Row("18711223", 966560000000L, "1", 364683600000L, "", "966", "", "2", 33, "", "", "", "", "", "", "7")
    val row2 = Row("subscriberId_2", 966561234567L, "genderTypeCd_2", 289180800000L, "ethtyTypeCd_2", "nationalityCd_2",
      "occupationCd_2", "langCd_2", 36, "address_2", "postalCd_2", "uaTerminalOs_2", "uaTerminalModel_2",
      "uaBrowserName_2", "vipNumber_2", "maritalStatusCd_2")
    val wrongRow = Row("18711223", "NAN", "1", 364683600000L, "", "966", "", "2", 33, "", "", "", "", "", "", "7")

    val rows = sc.parallelize(List(row, row2))
  }

  trait WithSubscriberIa {

    val subscriberIa1 =
      SubscriberIa(
        subscriberId = "18711223",
        msisdn = 966560000000L,
        genderTypeCd = "1",
        prmPartyBirthDt = Some(364683600000L),
        ethtyTypeCd = "",
        nationalityCd = "966",
        occupationCd = "",
        langCd = "2",
        age = 33,
        address = "",
        postalCd = "",
        uaTerminalOs = "",
        uaTerminalModel = "",
        uaBrowserName = "",
        vipNumber = "",
        maritalStatusCd = "7")
    val subscriberIa2 =
      SubscriberIa(
        subscriberId = "subscriberId_2",
        msisdn = 966561234567L,
        genderTypeCd = "genderTypeCd_2",
        prmPartyBirthDt = Some(289180800000L),
        ethtyTypeCd = "ethtyTypeCd_2",
        nationalityCd = "nationalityCd_2",
        occupationCd = "occupationCd_2",
        langCd = "langCd_2",
        age = 36,
        address = "address_2",
        postalCd = "postalCd_2",
        uaTerminalOs = "uaTerminalOs_2",
        uaTerminalModel = "uaTerminalModel_2",
        uaBrowserName = "uaBrowserName_2",
        vipNumber = "vipNumber_2",
        maritalStatusCd = "maritalStatusCd_2")
    val subscriberIa3 =
      SubscriberIa(
        subscriberId = "subscriberId_3",
        msisdn = 966563333333L,
        genderTypeCd = "genderTypeCd_3",
        prmPartyBirthDt = Some(794448000000L),
        ethtyTypeCd = "ethtyTypeCd_3",
        nationalityCd = "nationalityCd_3",
        occupationCd = "occupationCd_3",
        langCd = "langCd_3",
        age = 20,
        address = "address_3",
        postalCd = "postalCd_3",
        uaTerminalOs = "uaTerminalOs_3",
        uaTerminalModel = "uaTerminalModel_3",
        uaBrowserName = "uaBrowserName_3",
        vipNumber = "vipNumber_3",
        maritalStatusCd = "maritalStatusCd_3")

    val subscriberIa = sc.parallelize(List(subscriberIa1, subscriberIa2, subscriberIa3))
  }

  "SubscriberIaDsl" should "get correctly parsed data" in new WithSubscriberIaText {
    subscriber.toSubscriberIa.count should be (2)
  }

  it should "get errors when parsing data" in new WithSubscriberIaText {
    subscriber.toSubscriberIaErrors.count should be (1)
  }

  it should "get both correctly and wrongly parsed data" in new WithSubscriberIaText {
    subscriber.toParsedSubscriberIa.count should be (3)
  }

  it should "get correctly parsed rows" in new WithSubscriberIaRows {
    rows.toSubscriberIa.count should be (2)
  }

  it should "save in parquet" in new WithSubscriberIa {
    val path = File.makeTemp().name
    subscriberIa.saveAsParquetFile(path)
    sqc.parquetFile(path).toSubscriberIa.collect should be (subscriberIa.collect)
    File(path).deleteRecursively
  }
}
