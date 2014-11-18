/*
 * TODO: License goes here!
 */

package sa.com.mobily.cell

object Flickering {

  def detect(byUserSortedCells: Seq[(Long, (Int, Int))], timeElapsed: Long): Seq[Set[(Int, Int)]] = {
    byUserSortedCells.sliding(3).filter({
      case Seq(a, b, c) => (a._2 == c._2 && b._2 != a._2 && (c._1 - a._1) <= timeElapsed)
      case _ => false
    }).map({
      case Seq(a, b, c) => Set(a._2, b._2)
    }).toSeq.distinct
  }
}
