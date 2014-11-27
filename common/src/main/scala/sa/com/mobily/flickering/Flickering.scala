/*
 * TODO: License goes here!
 */

package sa.com.mobily.flickering

import annotation.tailrec

import sa.com.mobily.cell.Cell

object Flickering {

  def detect(
    byUserSortedCells: Seq[(Long, (Int, Int))],
    timeWindow: Long)
    (implicit cellCatalogue: Map[(Int, Int), Cell]): Set[FlickeringCells] = {

    @tailrec
    def detect(
        byUserSortedTimeCells: Seq[(Long, (Int, Int))],
        timeElapsed: Long,
        result: Set[FlickeringCells] = Set()): Set[FlickeringCells] = {
      if (byUserSortedTimeCells == Nil) result
      else {
        val cellAnalysis = byUserSortedTimeCells.head._2
        val cellsToAnalysis = byUserSortedTimeCells.tail
        val timeMax = byUserSortedTimeCells.head._1 + timeElapsed
        val timeCellsWithFlickering = cellsToAnalysis.take(cellsToAnalysis.lastIndexWhere(timeCell =>
          timeCell._1 <= timeMax && timeCell._2 == cellAnalysis))
        if (timeCellsWithFlickering.isEmpty) detect(byUserSortedTimeCells.tail, timeElapsed, result)
        else {
          val cellWithFlickering = cellCatalogue(cellAnalysis)
          val filterCellsIdWithFlickering = timeCellsWithFlickering.map(timeCell => timeCell._2).filter(cell =>
            cell != cellAnalysis)
          val flickeringCells = filterCellsIdWithFlickering.map(cell => {
            FlickeringCells(Set((cellWithFlickering.lacTac, cellWithFlickering.cellId), cell))
          })
          detect(byUserSortedTimeCells.tail, timeElapsed, result ++ flickeringCells)
        }
      }
    }

    detect(byUserSortedCells, timeWindow)
  }
}
