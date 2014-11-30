/*
 * TODO: License goes here!
 */

package sa.com.mobily.xdr

import org.scalatest._

class TransferStatsTest extends FlatSpec with ShouldMatchers {

  trait WithTransferStats {

    val header = Array[String](
      "l4UlThroughput",
      "l4DwThroughput",
      "l4UlPackets",
      "l4DwPackets",
      "dataTransUlDuration",
      "dataTransDwDuration",
      "ulLostRate",
      "dwLostRate")
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
    val t1Fields = Array[String]("10", "2000", "5", "574", "737", "8966", "1", "218")

    val t2 = TransferStats(
      l4UlThroughput = 232L,
      l4DwThroughput = 17814L,
      l4UlPackets = 674,
      l4DwPackets = 1299,
      dataTransUlDuration = 2889L,
      dataTransDwDuration = 45984L,
      ulLostRate = 33,
      dwLostRate = 1500)
    val t2Fields = Array[String]("232", "17814", "674", "1299", "2889", "45984", "33", "1500")

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

  it should "return t1 when t2 None" in new WithTransferStats {
    TransferStats.aggregate(Option(t1), None) should be (t1)
  }

  it should "return t2 when t1 None" in new WithTransferStats {
    TransferStats.aggregate(None, Option(t2)) should be (t2)
  }

  it should "return aggregation when t1 and t2 defined" in new WithTransferStats {
    TransferStats.aggregate(Option(t1), Option(t2)) should be (aggregatedTransferStats)
  }

  it should "return fields as String for t1" in new WithTransferStats {
    TransferStats.fields(t1) should be (t1Fields)
  }

  it should "return fields as String for t2" in new WithTransferStats {
    TransferStats.fields(t2) should be (t2Fields)
  }

  it should "return header as String" in new WithTransferStats {
    TransferStats.header should be (header)
  }
}
