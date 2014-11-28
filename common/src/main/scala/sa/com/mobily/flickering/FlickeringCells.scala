/*
 * TODO: License goes here!
 */

package sa.com.mobily.flickering

import sa.com.mobily.cell.Cell
import sa.com.mobily.geometry.GeomUtils

case class FlickeringCells(cells: Set[(Int, Int)] = Set()) {

  require(cells.size == 2)

  def fields(implicit cellCatalogue: Map[(Int, Int), Cell]): Array[String] = {
    val Array(cell1, cell2) = cells.toArray
    val aggGeom =
      cellCatalogue((cell1._1, cell1._2)).coverageGeom.union(cellCatalogue((cell2._1, cell2._2)).coverageGeom)
    Array(cell1._1.toString, cell1._2.toString, cell2._1.toString, cell2._2.toString, GeomUtils.wkt(aggGeom))
  }
}
