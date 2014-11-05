/*
 * TODO: License goes here!
 */

package sa.com.mobily.ia

case class TrafficInfo (
    visitCount: Long = 0L,
    uploadVolume: Double = 0D,
    downloadVolume: Double = 0D,
    totalVolume: Double = 0D)

object TrafficInfo {

  def aggregate(t1: Option[TrafficInfo], t2: Option[TrafficInfo]): TrafficInfo = {
    (t1, t2) match {
      case (None, None) => TrafficInfo()
      case (Some(t), None) => t
      case (None, Some(t)) => t
      case (Some(first), Some(second)) => TrafficInfo.aggregate(first, second)
    }
  }

  def aggregate(t1: TrafficInfo, t2: TrafficInfo): TrafficInfo = {
    TrafficInfo(
      visitCount = t1.visitCount + t2.visitCount,
      uploadVolume = t1.uploadVolume + t2.uploadVolume,
      downloadVolume = t1.downloadVolume + t2.downloadVolume,
      totalVolume = t1.totalVolume + t2.totalVolume)
  }
}
