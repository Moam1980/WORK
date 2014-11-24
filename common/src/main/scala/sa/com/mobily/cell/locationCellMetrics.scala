/*
 * TODO: License goes here!
 */

package sa.com.mobily.cell

case class LocationCellMetrics(
    cellIdentifier: (Int, Int),
    cellWkt: String,
    centroidDistance: Double,
    areaRatio: Double)

case class LocationCellAggMetrics(
    cellsWkt: List[String],
    numberOfCells: Long,
    centroidDistanceAvg: Double,
    centroidDistanceStDev: Double,
    centroidDistanceMin: Double,
    centroidDistanceMax: Double,
    areaRatioAvg: Double,
    areaRatioStDev: Double)
