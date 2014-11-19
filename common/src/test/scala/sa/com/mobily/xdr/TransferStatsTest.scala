/*
 * TODO: License goes here!
 */

package sa.com.mobily.xdr

import org.scalatest._

class TransferStatsTest extends FlatSpec with ShouldMatchers {

  trait WithTransferStats {

    val emptyTransferStats = TransferStats(
      l4UlThroughput = 0L,
      l4DwThroughput = 0L,
      l4UlPackets = 0,
      l4DwPackets = 0,
      dataTransUlDuration = 0L,
      dataTransDwDuration = 0L,
      ulLostRate = 0,
      dwLostRate = 0)
    val t1 = TransferStats(
      l4UlThroughput = 10L,
      l4DwThroughput = 2000L,
      l4UlPackets = 5,
      l4DwPackets = 574,
      dataTransUlDuration = 737L,
      dataTransDwDuration = 8966L,
      ulLostRate = 1,
      dwLostRate = 218)
    val t2 = TransferStats(
      l4UlThroughput = 232L,
      l4DwThroughput = 17814L,
      l4UlPackets = 674,
      l4DwPackets = 1299,
      dataTransUlDuration = 2889L,
      dataTransDwDuration = 45984L,
      ulLostRate = 33,
      dwLostRate = 1500)
    val aggregatedTransferStats = TransferStats(
      l4UlThroughput = 242L,
      l4DwThroughput = 19814L,
      l4UlPackets = 679,
      l4DwPackets = 1873,
      dataTransUlDuration = 3626L,
      dataTransDwDuration = 54950L,
      ulLostRate = 34,
      dwLostRate = 1718)
  }

  "TransferStats" should "return new Transfer Stats with all zeros when both are None" in new WithTransferStats {
    TransferStats.aggregate(None, None) should be (emptyTransferStats)
  }

  "TransferStats" should "return t1 when t2 None" in new WithTransferStats {
    TransferStats.aggregate(Option(t1), None) should be (t1)
  }

  "TransferStats" should "return t2 when t1 None" in new WithTransferStats {
    TransferStats.aggregate(None, Option(t2)) should be (t2)
  }

  "TransferStats" should "return aggregation when t1 and t2 defined" in new WithTransferStats {
    TransferStats.aggregate(Option(t1), Option(t2)) should be (aggregatedTransferStats)
  }
}
