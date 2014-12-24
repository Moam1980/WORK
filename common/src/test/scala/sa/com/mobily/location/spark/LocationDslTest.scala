/*
 * TODO: License goes here!
 */

package sa.com.mobily.location.spark

import org.scalatest._

import sa.com.mobily.utils.LocalSparkContext

class LocationDslTest extends FlatSpec with ShouldMatchers with LocalSparkContext {

  import LocationDsl._

  trait WithLocationText {

    val location1 = "\"0\"|\"locationTest\"|\"0\"|\"clientTest\"|\"EPSG:4326\"|\"POLYGON ((0 0, 0 2, 2 2, 2 0, 0 0))\""
    val location2 = "\"1\"|\"other\"|\"2\"|\"clientOther\"|\"EPSG:32638\"|\"POLYGON ((0 0, 0 2, 2 2, 2 0, 0 0))\""
    val location3 = "\"WrongNumber\"|\"other\"|\"2\"|\"clientOther\"|\"EPSG:32638\"|" +
      "\"POLYGON ((0 0, 0 2, 2 2, 2 0, 0 0))\""

    val location = sc.parallelize(List(location1, location2, location3))
  }

  "LocationDsl" should "get correctly parsed data" in new WithLocationText {
    location.toLocation.count should be (2)
  }

  it should "get errors when parsing data" in new WithLocationText {
    location.toLocationErrors.count should be (1)
  }

  it should "get both correctly and wrongly parsed data" in new WithLocationText {
    location.toParsedLocation.count should be (3)
  }
}
