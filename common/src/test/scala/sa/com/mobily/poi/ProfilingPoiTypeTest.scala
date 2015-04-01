/*
 * TODO: License goes here!
 */

package sa.com.mobily.userPoiType

import org.scalatest.{FlatSpec, ShouldMatchers}

import sa.com.mobily.poi._
import sa.com.mobily.utils.EdmCustomMatchers

class ProfilingPoiTypeTest extends FlatSpec with ShouldMatchers with EdmCustomMatchers {

  trait WithUserPoiTypes {

    val homeAndWorkOnly = List(Home, Work)
    val homeAndWork = List(Work, TypeOne, Home)

    val homeOnly = List(Home)
    val home = List(Home, TypeFive)

    val workOnly = List(Work)
    val work = List(TypeFour, Work)

    val othersOnly = List(TypeOne, TypeTwo, TypeThree, TypeFour, TypeFive, TypeSix)
  }

  "ProfilingPoiType" should "parse poi types to home and work" in new WithUserPoiTypes {
    ProfilingPoiType.parseUserPoiType(homeAndWorkOnly) should be (HomeAndWorkDefined)
    ProfilingPoiType.parseUserPoiType(homeAndWork) should be (HomeAndWorkDefined)
  }

  it should "parse poi types to home" in new WithUserPoiTypes {
    ProfilingPoiType.parseUserPoiType(homeOnly) should be (HomeDefined)
    ProfilingPoiType.parseUserPoiType(home) should be (HomeDefined)
  }

  it should "parse poi types to work" in new WithUserPoiTypes {
    ProfilingPoiType.parseUserPoiType(workOnly) should be (WorkDefined)
    ProfilingPoiType.parseUserPoiType(work) should be (WorkDefined)
  }

  it should "parse poi types to other" in new WithUserPoiTypes {
    ProfilingPoiType.parseUserPoiType(othersOnly) should be (OtherDefined)
  }
}
