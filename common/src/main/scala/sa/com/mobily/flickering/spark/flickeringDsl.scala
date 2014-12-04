/*
 * TODO: License goes here!
 */

package sa.com.mobily.flickering.spark

import scala.language.implicitConversions

import org.apache.spark.SparkContext._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import sa.com.mobily.cell.Cell
import sa.com.mobily.cell.spark.CellDsl
import sa.com.mobily.event.spark.EventDsl
import sa.com.mobily.flickering.{Flickering, FlickeringCells}

class FlickeringFunctions(self: RDD[FlickeringCells]) {

  def solvableFlickering(growthFactor: Float = 1.3f)
      (implicit cellCatalogue: Broadcast[Map[(Int, Int), Cell]]): RDD[(Double, FlickeringCells)] =
    flickeringAnalysis(growthFactor)(cellCatalogue).flatMap(cellsWithFlickering => cellsWithFlickering match {
      case Left(solvableFlickeringCells) => Seq(solvableFlickeringCells)
      case _ => Seq()
    })

  def nonSolvableFlickering(growthFactor: Float = 1.3f)
      (implicit cellCatalogue: Broadcast[Map[(Int, Int), Cell]]): RDD[FlickeringCells] =
    flickeringAnalysis(growthFactor)(cellCatalogue).flatMap(cellsWithFlickering => cellsWithFlickering match {
      case Right(nonSolvableFlickeringCells) => Seq(nonSolvableFlickeringCells)
      case _ => Seq()
    })

  def flickeringAnalysis(growthFactor: Float = 1.3f)
      (implicit cellCatalogue: Broadcast[Map[(Int, Int), Cell]]):
      RDD[Either[(Double, FlickeringCells), FlickeringCells]] = {
    filterNonIntersectedCells.map(flickeringCells =>
      Flickering.analysis(flickeringCells, growthFactor)(cellCatalogue.value))
  }

  def filterNonIntersectedCells(implicit bcCellCatalogue: Broadcast[Map[(Int, Int), Cell]]): RDD[FlickeringCells] = {
    self.filter(flickeringCells => {
      val cellCatalogue = bcCellCatalogue.value
      val Array(firstIds, secondIds) = flickeringCells.cells.toArray
      !cellCatalogue(firstIds).intersects(cellCatalogue(secondIds))
    })
  }

  def countFlickeringsByCell: RDD[((Int, Int), Int)] =
    self.flatMap(flickeringCells => flickeringCells.cells.map(cellId => (cellId, 1))).reduceByKey(_ + _)
}

trait FlickeringDsl {

  implicit def flickeringFunctions(flickeringCells: RDD[FlickeringCells]): FlickeringFunctions =
    new FlickeringFunctions(flickeringCells)
}

object FlickeringDsl extends FlickeringDsl with EventDsl with CellDsl
