/*
 * TODO: License goes here!
 */

package sa.com.mobily.cell

import org.scalatest.{FlatSpec, ShouldMatchers}

class LocationCellMetricsTest extends FlatSpec with ShouldMatchers {

  trait WithLocationCellMetrics {

    val cellMetrics =
      LocationCellMetrics(
        cellIdentifier = (57, 4465390),
        cellWkt = "POLYGON ((0 0, 0 2, 2 2, 2 0, 0 0))",
        cellArea = 4,
        technology = "4G_TDD",
        cellType = "MACRO",
        range = 2530.3,
        centroidDistance = 1,
        areaRatio = 1)
  }

  "LocationCellMetrics" should "provide fields for metrics" in new WithLocationCellMetrics {
    cellMetrics.fields should
      be (Array(
        "(57,4465390)",
        "4.0",
        "4G_TDD",
        "MACRO",
        "2530.3",
        "1.0",
        "1.0",
        "POLYGON ((0 0, 0 2, 2 2, 2 0, 0 0))"))
  }

  it should "have the same number of fields as in the header" in new WithLocationCellMetrics {
    cellMetrics.fields.size should be (LocationCellMetrics.Header.size)
  }

  it should "provide header field names" in new WithLocationCellMetrics {
    LocationCellMetrics.Header should
      be (Array("Cell Identifier", "Area", "Technology", "Type", "Range", "Centroid Distance", "Area Ratio", "WKT"))
  }
}
