/*
 * TODO: License goes here!
 */

package sa.com.mobily.ia

import org.apache.spark.sql.catalyst.expressions.Row
import org.scalatest._

class TrafficInfoTest extends FlatSpec with ShouldMatchers {

  import TrafficInfo._

  trait WithTrafficInfo {

    val emptyTrafficInfoFields = Array("0", "0.0", "0.0", "0.0")
    val trafficInfoHeader = Array("visitCount", "uploadVolume", "downloadVolume", "totalVolume")

    val row = Row(0L, 0D, 0D, 0D)
    val wrongRow = Row("NAN", 0D, 0D, 0D)
    
    val emptyTrafficInfo = TrafficInfo()
    val t1Fields = Array("10", "10.3", "20.8", "31.1")
    val t1 =
      TrafficInfo(
        visitCount = 10L,
        uploadVolume = 10.3D,
        downloadVolume = 20.8D,
        totalVolume = 31.1D)
    val t2Fields = Array("50", "100.777", "2.805", "103.582")
    val t2 =
      TrafficInfo(
        visitCount = 50L,
        uploadVolume = 100.777D,
        downloadVolume = 2.805D,
        totalVolume = 103.582D)
    val aggregatedTrafficInfo =
      TrafficInfo(
        visitCount = 60L,
        uploadVolume = 111.077D,
        downloadVolume = 23.605D,
        totalVolume = 134.682D)
  }

  "TrafficInfo" should "return correct header" in new WithTrafficInfo {
    TrafficInfo.Header should be (trafficInfoHeader)
  }

  it should "return correct fields" in new WithTrafficInfo {
    emptyTrafficInfo.fields should be (emptyTrafficInfoFields)
    t1.fields should be (t1Fields)
    t2.fields should be (t2Fields)
  }

  it should "have same number of elements fields and header" in new WithTrafficInfo {
    emptyTrafficInfo.fields.length should be (TrafficInfo.Header.length)
  }
  
  it should "return new Traffic info with all zeros when both are None" in new WithTrafficInfo {
    TrafficInfo.aggregate(None, None) should be (emptyTrafficInfo)
  }

  it should "return t1 when t2 None" in new WithTrafficInfo {
    TrafficInfo.aggregate(Option(t1), None) should be (t1)
  }

  it should "return t2 when t1 None" in new WithTrafficInfo {
    TrafficInfo.aggregate(None, Option(t2)) should be (t2)
  }

  it should "return aggregation when t1 and t2 definned" in new WithTrafficInfo {
    TrafficInfo.aggregate(Option(t1), Option(t2)) should be (aggregatedTrafficInfo)
  }

  it should "be built from Row" in new WithTrafficInfo {
    fromRow.fromRow(row) should be (emptyTrafficInfo)
  }

  it should "be discarded when row is wrong" in new WithTrafficInfo {
    an[Exception] should be thrownBy fromRow.fromRow(wrongRow)
  }
}
