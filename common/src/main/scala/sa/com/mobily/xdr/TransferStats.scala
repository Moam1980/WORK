/*
 * TODO: License goes here!
 */

package sa.com.mobily.xdr

case class TransferStats(
    l4UlThroughput: Long = 0L,
    l4DwThroughput: Long = 0L,
    l4UlPackets: Int = 0,
    l4DwPackets: Int = 0,
    dataTransUlDuration: Long = 0L,
    dataTransDwDuration: Long = 0L,
    ulLostRate: Int = 0,
    dwLostRate: Int = 0) {

  def fields: Array[String] =
    Array(
      l4UlThroughput.toString,
      l4DwThroughput.toString,
      l4UlPackets.toString,
      l4DwPackets.toString,
      dataTransUlDuration.toString,
      dataTransDwDuration.toString,
      ulLostRate.toString,
      dwLostRate.toString)
}

object TransferStats {

  def header: Array[String] =
    Array(
      "l4UlThroughput",
      "l4DwThroughput",
      "l4UlPackets",
      "l4DwPackets",
      "dataTransUlDuration",
      "dataTransDwDuration",
      "ulLostRate",
      "dwLostRate")

  def aggregate(t1: Option[TransferStats], t2: Option[TransferStats]): TransferStats = {
    (t1, t2) match {
      case (None, None) => TransferStats()
      case (Some(t), None) => t
      case (None, Some(t)) => t
      case (Some(first), Some(second)) => TransferStats.aggregate(first, second)
    }
  }

  def aggregate(t1: TransferStats, t2: TransferStats): TransferStats = {
    TransferStats(
      l4UlThroughput = t1.l4UlThroughput + t2.l4UlThroughput,
      l4DwThroughput = t1.l4DwThroughput + t2.l4DwThroughput,
      l4UlPackets = t1.l4UlPackets + t2.l4UlPackets,
      l4DwPackets = t1.l4DwPackets + t2.l4DwPackets,
      dataTransUlDuration = t1.dataTransUlDuration + t2.dataTransUlDuration,
      dataTransDwDuration = t1.dataTransDwDuration + t2.dataTransDwDuration,
      ulLostRate = t1.ulLostRate + t2.ulLostRate,
      dwLostRate = t1.dwLostRate + t2.dwLostRate)
  }
}
