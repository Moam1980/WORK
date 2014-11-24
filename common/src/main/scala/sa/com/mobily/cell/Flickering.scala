/*
 * TODO: License goes here!
 */

package sa.com.mobily.cell

import annotation.tailrec

object Flickering {

  def detect(byUserSortedCells: Seq[(Long, (Int, Int))], timeWindow: Long): Set[Set[(Int, Int)]] = {

    @tailrec
    def detect(
        byUserSortedTimeCells: Seq[(Long, (Int, Int))],
        timeElapsed: Long,
        result: Set[Set[(Int, Int)]] = Set()): Set[Set[(Int, Int)]] = {
      if (byUserSortedTimeCells == Nil) result
      else {
        val cellAnalysis = byUserSortedTimeCells.head._2
        val cellsToAnalysis = byUserSortedTimeCells.tail
        val timeMax = byUserSortedTimeCells.head._1 + timeElapsed
        val timeCellsWithFlickering = cellsToAnalysis.take(cellsToAnalysis.lastIndexWhere(timeCell =>
          timeCell._1 <= timeMax && timeCell._2 == cellAnalysis))
        if (timeCellsWithFlickering.isEmpty) detect(byUserSortedTimeCells.tail, timeElapsed, result)
        else {
          val cellsWithFlickering = timeCellsWithFlickering.map(timeCell => timeCell._2).filter(cell =>
            cell != cellAnalysis).map(cell => Set(cellAnalysis, cell))
          detect(byUserSortedTimeCells.tail, timeElapsed, result ++ cellsWithFlickering)
        }
      }
    }

    detect(byUserSortedCells, timeWindow)
  }
}
