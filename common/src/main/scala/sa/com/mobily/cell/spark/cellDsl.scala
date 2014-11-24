/*
 * TODO: License goes here!
 */

package sa.com.mobily.cell.spark

import scala.language.implicitConversions

import com.vividsolutions.jts.geom.Geometry
import com.vividsolutions.jts.simplify.DouglasPeuckerSimplifier
import org.apache.spark.SparkContext._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import sa.com.mobily.cell._
import sa.com.mobily.geometry.{GeomUtils, CellCoverage}
import sa.com.mobily.parsing.{ParsingError, ParsedItem}
import sa.com.mobily.parsing.spark.{SparkParser, ParsedItemsDsl}

class SqmFunctions(self: RDD[SqmCell]) {

  def toCell(bts: RDD[EgBts]): RDD[Cell] = CellMerger.merge(self, bts)
}

class CellReader(self: RDD[String]) {

  import ParsedItemsDsl._

  def toParsedCell: RDD[ParsedItem[Cell]] = SparkParser.fromCsv[Cell](self)

  def toCell: RDD[Cell] = toParsedCell.values

  def toCellErrors: RDD[ParsingError] = toParsedCell.errors
}

class CellFunctions(self: RDD[Cell]) {

  def toBroadcastMap: Broadcast[Map[(Int, Int), Cell]] =
    self.sparkContext.broadcast(self.keyBy(c => (c.lacTac, c.cellId)).collect.toMap)

  def lacGeometries: RDD[(Int, Geometry)] = {
    val byLac = self.keyBy(_.lacTac).groupByKey
    byLac.mapValues(cellsForLac => {
      cellsForLac.foldLeft(cellsForLac.head.coverageGeom) { (geomAccum, cell) => geomAccum.union(cell.coverageGeom) }
    }).map(byLac => (byLac._1, DouglasPeuckerSimplifier.simplify(byLac._2, 1 / byLac._2.getPrecisionModel.getScale)))
  }

  def locationCellMetrics(location: Geometry): RDD[LocationCellMetrics] =
    self.filter(_.coverageGeom.intersects(location)).map(cell =>
      LocationCellMetrics(
        cellIdentifier = cell.identifier,
        cellWkt = cell.coverageWkt,
        centroidDistance = cell.centroidDistance(location),
        areaRatio = cell.areaRatio(location)))

  def locationCellAggMetrics(location: Geometry): LocationCellAggMetrics = {
    val cellMetrics = locationCellMetrics(location).cache
    val numberOfCells = cellMetrics.count

    val centroidDistances = cellMetrics.map(_.centroidDistance)
    val centroidDistanceAvg = centroidDistances.sum / numberOfCells
    val centroidDistanceMin = centroidDistances.min
    val centroidDistanceMax = centroidDistances.max
    val centroidDistanceSqrDiffs =
      centroidDistances.map(centroidDistance => math.pow(centroidDistance - centroidDistanceAvg, 2))
    val centroidDistanceStDev = Math.sqrt(centroidDistanceSqrDiffs.sum / numberOfCells)

    val areaRatios = cellMetrics.map(_.areaRatio)
    val areaRatioAvg = areaRatios.sum / numberOfCells
    val areaRatioSqrDiffs = areaRatios.map(areaRatio => math.pow(areaRatio - areaRatioAvg, 2))
    val areaRatioStDev = Math.sqrt(areaRatioSqrDiffs.sum / numberOfCells)

    cellMetrics.unpersist(blocking = false)

    LocationCellAggMetrics(
      cellsWkt = cellMetrics.map(_.cellWkt).collect.toList,
      numberOfCells = numberOfCells,
      centroidDistanceAvg = centroidDistanceAvg,
      centroidDistanceStDev = centroidDistanceStDev,
      centroidDistanceMin = centroidDistanceMin,
      centroidDistanceMax = centroidDistanceMax,
      areaRatioAvg = areaRatioAvg,
      areaRatioStDev = areaRatioStDev)
  }
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
        lacTac = sqmCell.lacTac,
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

  implicit def cellReader(csv: RDD[String]): CellReader = new CellReader(csv)

  implicit def sqmFunctions(sqmCells: RDD[SqmCell]): SqmFunctions = new SqmFunctions(sqmCells)

  implicit def cellFunctions(cells: RDD[Cell]): CellFunctions = new CellFunctions(cells)
}

object CellDsl extends CellDsl
