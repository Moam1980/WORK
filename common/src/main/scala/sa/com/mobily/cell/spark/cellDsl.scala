/*
 * TODO: License goes here!
 */

package sa.com.mobily.cell.spark

import scala.language.implicitConversions

import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

import sa.com.mobily.cell.{EgBts, SqmCell, Cell}
import sa.com.mobily.geometry.{GeomUtils, CellCoverage}

class SqmFunctions(self: RDD[SqmCell]) {

  def toCell(bts: RDD[EgBts]): RDD[Cell] = CellMerger.merge(self, bts)
}

object CellMerger {

  private val DefaultCellBeamwidthOverlapFactor = 1.2 // 10% at each side
  private val DegreesInCircumference = 360d

  def merge(
      sqmCells: RDD[SqmCell],
      btsSites: RDD[EgBts],
      cellBeamwidthOverlapFactor: Double = DefaultCellBeamwidthOverlapFactor): RDD[Cell] = {
    val sqmCellsPerSite = sqmCells.keyBy(s => (s.nodeId, s.lacTac)).groupByKey

    val sqmCellsWithBeamwidth = sqmCellsPerSite.flatMap { sqmCellsGroupedBySite =>
      val azimuths = computeBeamwidths(sqmCellsGroupedBySite._2.map(_.azimuth).toList, cellBeamwidthOverlapFactor)
      sqmCellsGroupedBySite._2.map(sqmCell => (sqmCell, azimuths.get(sqmCell.azimuth)))
    }

    val sqmCellsWithBeamwidthAndSite = sqmCellsWithBeamwidth.keyBy(s => (s._1.nodeId, s._1.lacTac)).join(
      btsSites.keyBy(b => (b.bts, b.lac))).map(_._2)

    sqmCellsWithBeamwidthAndSite.map(sqmCellWithBeamwidthAndSite => {
      val sqmCell = sqmCellWithBeamwidthAndSite._1._1
      val beamwidth = sqmCellWithBeamwidthAndSite._1._2.getOrElse(DegreesInCircumference)
      val btsSite = sqmCellWithBeamwidthAndSite._2
      val range = btsSite.outdoorCov
      val coverageGeom = CellCoverage.cellShape(
        cellLocation = sqmCell.basePlanarCoords.geometry,
        azimuth = sqmCell.azimuth,
        beamwidth = beamwidth,
        range = range)

      Cell(
        cellId = sqmCell.cellId,
        lac = sqmCell.lacTac,
        planarCoords = sqmCell.basePlanarCoords,
        technology = sqmCell.technology,
        cellType = sqmCell.cellType,
        height = sqmCell.height,
        azimuth = sqmCell.azimuth,
        beamwidth = beamwidth,
        range = range,
        coverageWkt = GeomUtils.wkt(coverageGeom),
        mcc = btsSite.cgi.substring(Cell.MccStartIndex, Cell.MncStartIndex),
        mnc = btsSite.cgi.substring(Cell.MncStartIndex, Cell.LacStartIndexMnc2Digits))
    })
  }

  def computeBeamwidths(
      azimuths: List[Double],
      cellBeamwidthOverlapFactor: Double = DefaultCellBeamwidthOverlapFactor): Map[Double, Double] =
    azimuths.foldLeft(Map[Double, (Double, Double)]()) { (accum, angle) =>
      val upperRange =
        if (angle == azimuths.max) DegreesInCircumference - angle + azimuths.min
        else azimuths.filter(_ > angle).sorted.head - angle
      val lowerRange =
        if (angle == azimuths.min) angle + DegreesInCircumference - azimuths.max
        else angle - azimuths.filter(_ < angle).sorted.last
      accum + (angle -> (lowerRange / 2, upperRange / 2)) // Only half of the range will be for this cell
    }.mapValues(lowUpRange => {
      val greaterRange = if (lowUpRange._1 > lowUpRange._2) lowUpRange._1 else lowUpRange._2
      val azimuth = greaterRange * 2 * cellBeamwidthOverlapFactor
      if (azimuth > DegreesInCircumference) DegreesInCircumference else azimuth
    })
}

trait CellDsl {

  implicit def sqmFunctions(sqmCells: RDD[SqmCell]): SqmFunctions = new SqmFunctions(sqmCells)
}

object CellDsl extends CellDsl
