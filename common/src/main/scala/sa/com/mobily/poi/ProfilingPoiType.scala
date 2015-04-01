/*
 * TODO: License goes here!
 */

package sa.com.mobily.poi

sealed case class ProfilingPoiType(identifier: String)

object ProfilingPoiType {

  def parseUserPoiType(poiTypes: List[PoiType]): ProfilingPoiType = {
    if (poiTypes.contains(Home) && poiTypes.contains(Work)) HomeAndWorkDefined
    else if (poiTypes.contains(Home)) HomeDefined
    else if (poiTypes.contains(Work)) WorkDefined
    else OtherDefined
  }
}

object HomeAndWorkDefined extends ProfilingPoiType(identifier = "Home & Work")
object HomeDefined extends ProfilingPoiType(identifier = "Home")
object WorkDefined extends ProfilingPoiType(identifier = "Work")
object OtherDefined extends ProfilingPoiType(identifier = "Other")
