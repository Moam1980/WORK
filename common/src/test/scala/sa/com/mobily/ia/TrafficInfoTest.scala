/*
 * TODO: License goes here!
 */

package sa.com.mobily.ia

import org.scalatest._

class TrafficInfoTest extends FlatSpec with ShouldMatchers {

  import TrafficInfo._

  trait WithTrafficInfo {

    val emptyTrafficInfo = TrafficInfo(
      visitCount = 0L,
      uploadVolume = 0.0D,
      downloadVolume = 0.0D,
      totalVolume = 0.0D)
    val t1 = TrafficInfo(
      visitCount = 10L,
      uploadVolume = 10.3D,
      downloadVolume = 20.8D,
      totalVolume = 31.1D)
    val t2 = TrafficInfo(
      visitCount = 50L,
      uploadVolume = 100.777D,
      downloadVolume = 2.805D,
      totalVolume = 103.582D)
    val aggregatedTrafficInfo = TrafficInfo(
      visitCount = 60L,
      uploadVolume = 111.077D,
      downloadVolume = 23.605D,
      totalVolume = 134.682D)
  }

  "TrafficInfo" should "return new Traffic info with all zeros when both are None" in new WithTrafficInfo {
    TrafficInfo.aggregate(None, None) should be (emptyTrafficInfo)
  }

  "TrafficInfo" should "return t1 when t2 None" in new WithTrafficInfo {
    TrafficInfo.aggregate(Option(t1), None) should be (t1)
  }

  "TrafficInfo" should "return t2 when t1 None" in new WithTrafficInfo {
    TrafficInfo.aggregate(None, Option(t2)) should be (t2)
  }

  "TrafficInfo" should "return aggregation when t1 and t2 definned" in new WithTrafficInfo {
    TrafficInfo.aggregate(Option(t1), Option(t2)) should be (aggregatedTrafficInfo)
  }
}
