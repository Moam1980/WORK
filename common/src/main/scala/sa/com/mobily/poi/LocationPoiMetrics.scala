/*
 * TODO: License goes here!
 */

package sa.com.mobily.poi

import scala.collection.Map

case class LocationPoiMetrics(
    intersectionRatioMean: Double,
    intersectionRatioStdev: Double,
    intersectionRatioMax: Double,
    intersectionRatioMin: Double,
    numUsers: Long,
    numPois: Long,
    numPoisPerTypeCombination: Map[Seq[PoiType], Long])
