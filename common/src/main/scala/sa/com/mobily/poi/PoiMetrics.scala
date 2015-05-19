/*
 * TODO: License goes here!
 */

package sa.com.mobily.poi

import scala.collection.Map

import sa.com.mobily.utils.Stats

case class PoiMetrics(
    numUsers: Long,
    numPois: Long,
    numUsersPerTypeCombination: Map[Seq[PoiType], Long],
    distancePoisPerTypeCombination: Map[Seq[PoiType], Stats],
    distancePoisSubPolygonsStats: Map[PoiType, Stats])
