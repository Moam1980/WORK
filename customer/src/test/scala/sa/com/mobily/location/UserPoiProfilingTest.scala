/*
 * TODO: License goes here!
 */

package sa.com.mobily.location

import org.apache.spark.sql._
import org.scalatest.{FlatSpec, ShouldMatchers}

import sa.com.mobily.parsing.{CsvParser, RowParser}
import sa.com.mobily.poi.{Work, Home, HomeAndWorkDefined}

class UserPoiProfilingTest extends FlatSpec with ShouldMatchers {

  import UserPoiProfiling._

  trait WithUserPoiProfiling {

    val line = "420030100040377|Home & Work"

    val row = Row("420030100040377", Row("Home & Work"))
    val wrongRow = Row("420030100040377", "Home & Work")

    val fields: Array[String] = Array("420030100040377", "Home & Work")
    val header: Array[String] = Array("imsi", "poiProfiling")

    val userPoiProfiling = UserPoiProfiling(
      imsi = "420030100040377",
      poiProfiling = HomeAndWorkDefined)
  }

  "UserPoiProfiling" should "be built from CSV" in new WithUserPoiProfiling {
    CsvParser.fromLine(line).value.get should be (userPoiProfiling)
  }

  it should "be built from Row" in new WithUserPoiProfiling {
    RowParser.fromRow(row) should be (userPoiProfiling)
  }

  it should "be discarded when Row format is wrong" in new WithUserPoiProfiling {
    an [Exception] should be thrownBy fromRow.fromRow(wrongRow)
  }

  it should "return correct fields" in new WithUserPoiProfiling {
    userPoiProfiling.fields should be (fields)
  }

  it should "return correct header" in new WithUserPoiProfiling {
    UserPoiProfiling.Header should be (header)
  }

  it should "field and header have same size" in new WithUserPoiProfiling {
    userPoiProfiling.fields.size should be (UserPoiProfiling.Header.size)
  }

  it should "apply UserPoiProfiling built from imsi and list of Poi Types" in new WithUserPoiProfiling {
    UserPoiProfiling("420030100040377", List(Home, Work)) should be (userPoiProfiling)
  }
}
