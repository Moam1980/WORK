/*
 * TODO: License goes here!
 */

package sa.com.mobily.usercentric.spark

import org.scalatest.{FlatSpec, ShouldMatchers}

import sa.com.mobily.cell.{Micro, FourGFdd, Cell}
import sa.com.mobily.event.Event
import sa.com.mobily.geometry.UtmCoordinates
import sa.com.mobily.user.User
import sa.com.mobily.usercentric.{CompatibilityScore, SpatioTemporalSlot}
import sa.com.mobily.utils.LocalSparkContext

class UserModelDslTest extends FlatSpec with ShouldMatchers with LocalSparkContext {

  import UserModelDsl._

  trait WithEvents {

    val event1 = Event(
      User(imei = "1", imsi = "1", msisdn = 1),
      beginTime = 1,
      endTime = 2,
      lacTac = 1,
      cellId = 1,
      eventType = "859",
      subsequentLacTac = Some(0),
      subsequentCellId = Some(0),
      inSpeed = Some(0),
      outSpeed = Some(3),
      minSpeedPointWkt = Some("POINT (1 1)"))
    val event2 = event1.copy(beginTime = 3, endTime = 4, cellId = 2)
    val event3 = event1.copy(beginTime = 5, endTime = 6, cellId = 3)
    val event4 = event1.copy(beginTime = 7, endTime = 8, cellId = 1)

    val events = sc.parallelize(Array(event1, event2, event3))
    val withSameCellEvents = sc.parallelize(Array(event1, event1, event2, event3, event3, event3, event4))
  }

  trait WithCellCatalogue {

    val cell1 = Cell(1, 1, UtmCoordinates(1, 4), FourGFdd, Micro, 20, 180, 45, 4,
      "POLYGON ((0 0, 0 2, 2 2, 2 0, 0 0))")
    val cell2 = cell1.copy(cellId = 2, coverageWkt = "POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))")
    val cell3 = cell1.copy(cellId = 3, coverageWkt = "POLYGON ((0.5 0, 0.5 1, 1.5 1, 1.5 0, 0.5 0))")

    implicit val cellCatalogue = Map((1, 1) -> cell1, (1, 2) -> cell2, (1, 3) -> cell3)
    implicit val bcCellCatalogue = sc.parallelize(Array(cell1, cell2, cell3)).toBroadcastMap
  }

  trait WithSpatioTemporalSlots extends WithEvents {

    val slot1 = SpatioTemporalSlot(
      userId = 1,
      startTime = 1,
      endTime = 2,
      geomWkt = "POLYGON ((0 0, 0 2, 2 2, 2 0, 0 0))",
      cells = Set((1, 1)))
    val slot2 = SpatioTemporalSlot(
      userId = 1,
      startTime = 2,
      endTime = 5,
      geomWkt = "POLYGON ((0 0, 0 2, 2 2, 2 0, 0 0))",
      cells = Set((1, 2)))
    val slot1WithScore = slot1.copy(score = Some(CompatibilityScore(1, 0)))
  }

  trait WithCompatibilitySlots extends WithEvents {

    val prefixWkt = "POLYGON ((0 2, 1 2, 1 3, 0 3, 0 2))"
    val slot1Wkt = "POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))"
    val slot2Wkt = "POLYGON ((0.5 0, 1.5 0, 1.5 1, 0.5 1, 0.5 0))"
    val suffixWkt = "POLYGON ((2 0, 2 1, 3 1, 3 0, 2 0))"
    val mergedWkt = "POLYGON ((1 0, 0.5 0, 0.5 1, 1 1, 1 0))"
    val slot1AfterWkt = "POLYGON ((20 20, 21 20, 21 21, 20 21, 20 20))"
    val slot2AfterWkt = "POLYGON ((20.5 20, 21.5 20, 21.5 21, 20.5 21, 20.5 20))"
    val suffix2Wkt = "POLYGON ((22 20, 22 21, 23 21, 23 20, 22 20))"
    val merged2Wkt = "POLYGON ((21 20, 20.5 20, 20.5 21, 21 21, 21 20))"

    val prefixSlot = SpatioTemporalSlot(
      userId = 1,
      startTime = 0,
      endTime = 10,
      geomWkt = prefixWkt,
      cells = Set((1, 1), (1, 2)),
      score = Some(CompatibilityScore(0, 0)))
    val slot1 = prefixSlot.copy(
      startTime = 10,
      endTime = 20,
      geomWkt = slot1Wkt,
      score = Some(CompatibilityScore(0.5, 0)))
    val slot2 = prefixSlot.copy(
      startTime = 20,
      endTime = 30,
      geomWkt = slot2Wkt,
      score = Some(CompatibilityScore(0, 0)))
    val suffixSlot = prefixSlot.copy(startTime = 30, endTime = 40, geomWkt = suffixWkt, score = None)

    val mergedSlot = slot1.copy(
      endTime = 30,
      geomWkt = mergedWkt,
      cells = Set((1, 1), (1, 2)),
      score = Some(CompatibilityScore(0, 0)))

    val slot1After = prefixSlot.copy(
      startTime = 40,
      endTime = 50,
      geomWkt = slot1AfterWkt,
      score = Some(CompatibilityScore(0.5, 0)))
    val slot2After = prefixSlot.copy(
      startTime = 50,
      endTime = 60,
      geomWkt = slot2AfterWkt,
      score = Some(CompatibilityScore(0, 0)))
    val suffixSlot2 = prefixSlot.copy(startTime = 60, endTime = 70, geomWkt = suffix2Wkt, score = None)

    val merged2Slot = slot1After.copy(
      endTime = 60,
      geomWkt = merged2Wkt,
      cells = Set((1, 1), (1, 2)),
      score = Some(CompatibilityScore(0, 0)))

    val slots =
      sc.parallelize(Array((1L, List(prefixSlot, slot1, slot2, suffixSlot, slot1After, slot2After, suffixSlot2))))
    val onlySlot = sc.parallelize(Array((1L, List(prefixSlot))))
  }

  "UserModelDsl" should "not aggregate when there are no consecutive events having the same cell" in
    new WithSpatioTemporalSlots with WithCellCatalogue {
      events.byUserChronologically.aggSameCell.first._2.size should be (3)
    }

  it should "aggregate consecutive events having the same cell" in
    new WithSpatioTemporalSlots with WithCellCatalogue {
      withSameCellEvents.byUserChronologically.aggSameCell.first._2.size should be(4)
    }

  it should "merge compatible slots" in new WithCompatibilitySlots {
    slots.combine.first._2.size should be(5)
  }

  it should "merge no slots when there is only one" in new WithCompatibilitySlots {
    onlySlot.combine.first._2.size should be(1)
  }
}
