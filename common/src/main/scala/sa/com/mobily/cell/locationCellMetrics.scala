/*
 * TODO: License goes here!
 */

package sa.com.mobily.cell

case class LocationCellMetrics(
    cellIdentifier: (Int, Int),
    cellWkt: String,
    cellArea: Double,
    technology: String,
    cellType: String,
    range: Double,
    centroidDistance: Double,
    areaRatio: Double) {

  def fields: Array[String] =
    Array(
      cellIdentifier.toString,
      cellArea.toString,
      technology,
      cellType,
      range.toString,
      centroidDistance.toString,
      areaRatio.toString,
      cellWkt)
}

object LocationCellMetrics {

  val Header: Array[String] =
    Array("Cell Identifier", "Area", "Technology", "Type", "Range", "Centroid Distance", "Area Ratio", "WKT")
}

case class LocationCellAggMetrics(
    cellsWkt: List[String],
    numberOfCells: Long,
    centroidDistanceAvg: Double,
    centroidDistanceStDev: Double,
    centroidDistanceMin: Double,
    centroidDistanceMax: Double,
    areaRatioAvg: Double,
    areaRatioStDev: Double)
