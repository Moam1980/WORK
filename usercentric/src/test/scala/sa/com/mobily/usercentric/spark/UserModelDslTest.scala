/*
 * TODO: License goes here!
 */

package sa.com.mobily.usercentric.spark

import org.scalatest.{FlatSpec, ShouldMatchers}

import sa.com.mobily.cell.{Micro, FourGFdd, Cell}
import sa.com.mobily.event.{PsEventSource, Event}
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
      cellId = 2,
      source = PsEventSource,
      eventType = Some("859"),
      subsequentLacTac = Some(0),
      subsequentCellId = Some(0),
      inSpeed = Some(0),
      outSpeed = Some(3),
      minSpeedPointWkt = Some("POINT (1 1)"))
    val event2 = event1.copy(beginTime = 3, endTime = 4, cellId = 3, inSpeed = Some(3))
    val event3 = event1.copy(beginTime = 5, endTime = 6, cellId = 5, inSpeed = Some(3))
    val event4 = event1.copy(beginTime = 1, endTime = 3, cellId = 6, inSpeed = Some(3))
    val event5 = event1.copy(beginTime = 8, endTime = 10, cellId = 7)
    val events = sc.parallelize(Array(event1, event2, event3, event5))
    val withSameCellEvents = sc.parallelize(Array(event1, event1, event2, event3, event3, event3, event4, event5))
  }

  trait WithCellCatalogue {

    val cellPrefixWkt = "POLYGON ((0 20, 10 20, 10 30, 0 30, 0 20))"
    val cellSlot1Wkt = "POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))"
    val cellSlot2Wkt = "POLYGON ((5 0, 15 0, 15 10, 5 10, 5 0))"
    val cellSuffixWkt = "POLYGON ((20 0, 20 10, 30 10, 30 0, 20 0))"
    val slot1AfterWkt = "POLYGON ((200 200, 210 200, 210 210, 200 210, 200 200))"
    val slot2AfterWkt = "POLYGON ((205 200, 215 200, 215 210, 205 210, 205 200))"
    val suffix2Wkt = "POLYGON ((220 200, 220 210, 230 210, 230 200, 220 200))"

    val cellPrefix = Cell(1, 1, UtmCoordinates(1, 4), FourGFdd, Micro, 20, 180, 45, 4, "1", cellPrefixWkt)
    val cellSlot1 = cellPrefix.copy(cellId = 2, coverageWkt = cellSlot1Wkt)
    val cellSlot2 = cellPrefix.copy(cellId = 3, coverageWkt = cellSlot2Wkt)
    val cellSuffix = cellPrefix.copy(cellId = 4, coverageWkt = cellSuffixWkt)
    val cellSlot1After = cellPrefix.copy(cellId = 5, coverageWkt = slot1AfterWkt)
    val cellSlot2After = cellPrefix.copy(cellId = 6, coverageWkt = slot2AfterWkt)
    val cellSuffix2 = cellPrefix.copy(cellId = 7, coverageWkt = suffix2Wkt)

    implicit val bcCellCatalogue = sc.parallelize(
      Array(cellPrefix, cellSlot1, cellSlot2, cellSuffix, cellSlot1After, cellSlot2After, cellSuffix2)).toBroadcastMap
  }

  trait WithSpatioTemporalSlots extends WithEvents {

    val slot1 = SpatioTemporalSlot(
      user = User("", "", 1),
      startTime = 1,
      endTime = 2,
      cells = Set((1, 1)),
      firstEventBeginTime = 1,
      lastEventEndTime = 2,
      outMinSpeed = 0.5,
      intraMinSpeedSum = 0,
      numEvents = 1)
    val slot2 = SpatioTemporalSlot(
      user = User("", "", 1),
      startTime = 2,
      endTime = 5,
      cells = Set((1, 2)),
      firstEventBeginTime = 2,
      lastEventEndTime = 5,
      outMinSpeed = 0,
      intraMinSpeedSum = 0,
      numEvents = 1)
    val slot1WithScore = slot1.copy(score = Some(CompatibilityScore(1, 0, 0.5)))
  }

  trait WithCompatibilitySlots extends WithEvents {

    val prefixSlot = SpatioTemporalSlot(
      user = User("", "", 1),
      startTime = 0,
      endTime = 10,
      cells = Set((1, 1)),
      firstEventBeginTime = 0,
      lastEventEndTime = 10,
      outMinSpeed = 0,
      intraMinSpeedSum = 0,
      numEvents = 1,
      score = Some(CompatibilityScore(0, 0, 0)))
    val slot1 = prefixSlot.copy(
      startTime = 10,
      endTime = 20,
      cells = Set((1, 2)),
      firstEventBeginTime = 10,
      lastEventEndTime = 20,
      score = Some(CompatibilityScore(0.5, 0, 0)))
    val slot2 = prefixSlot.copy(
      startTime = 20,
      endTime = 30,
      cells = Set((1, 3)),
      firstEventBeginTime = 20,
      lastEventEndTime = 30,
      score = Some(CompatibilityScore(0, 0, 0)))
    val suffixSlot = prefixSlot.copy(
      startTime = 30,
      endTime = 40,
      cells = Set((1, 4)),
      firstEventBeginTime = 30,
      lastEventEndTime = 40,
      score = None)

    val mergedSlot = slot1.copy(
      endTime = 30,
      cells = Set((1, 2), (1, 3)),
      lastEventEndTime = 30,
      outMinSpeed = 0,
      intraMinSpeedSum = 0,
      numEvents = 2,
      score = Some(CompatibilityScore(0, 0, 0)))

    val slot1After = prefixSlot.copy(
      startTime = 40,
      endTime = 50,
      cells = Set((1, 5)),
      firstEventBeginTime = 40,
      lastEventEndTime = 50,
      score = Some(CompatibilityScore(0.5, 0, 0)))
    val slot2After = prefixSlot.copy(
      startTime = 50,
      endTime = 60,
      cells = Set((1, 6)),
      firstEventBeginTime = 50,
      lastEventEndTime = 60,
      score = Some(CompatibilityScore(0, 0, 0)))
    val suffixSlot2 = prefixSlot.copy(
      startTime = 60,
      endTime = 70,
      cells = Set((1, 7)),
      firstEventBeginTime = 60,
      lastEventEndTime = 70,
      score = None)

    val merged2Slot = slot1After.copy(
      endTime = 60,
      cells = Set((1, 5), (1, 6)),
      lastEventEndTime = 60,
      outMinSpeed = 0,
      intraMinSpeedSum = 0,
      numEvents = 2,
      score = Some(CompatibilityScore(0, 0, 0)))

    val slots = sc.parallelize(
      Array((User("", "", 1L), List(prefixSlot, slot1, slot2, suffixSlot, slot1After, slot2After, suffixSlot2))))
    val onlySlot = sc.parallelize(Array((User("", "", 1L), List(prefixSlot))))
  }

  trait WithModelSlots extends WithCellCatalogue {

    val slot1 = SpatioTemporalSlot(
      user = User("", "", 1),
      startTime = 1,
      endTime = 2,
      cells = Set((1, 1)),
      firstEventBeginTime = 1,
      lastEventEndTime = 2,
      outMinSpeed = 7.5,
      intraMinSpeedSum = 0.5,
      numEvents = 3)
    val slotJvp1 = slot1.copy(
      startTime = 2,
      endTime = 3,
      cells = Set((1, 2)),
      firstEventBeginTime = 2,
      lastEventEndTime = 3,
      outMinSpeed = 10)
    val slotJvp2 = slot1.copy(
      startTime = 5,
      endTime = 7,
      cells = Set((1, 3)),
      firstEventBeginTime = 5,
      lastEventEndTime = 6,
      outMinSpeed = 8)
    val slot2 = slot1.copy(
      startTime = 10,
      endTime = 15,
      cells = Set((1, 4)),
      firstEventBeginTime = 12,
      lastEventEndTime = 13,
      outMinSpeed = 0)
    val slot3 = slot1.copy(
      startTime = 30,
      endTime = 35,
      cells = Set((1, 5)),
      firstEventBeginTime = 32,
      lastEventEndTime = 33,
      outMinSpeed = 0)

    val slots = sc.parallelize(Array((User("", "", 1L), List(slot1, slotJvp1, slotJvp2, slot2, slot3))))
  }

  "UserModelDsl" should "not aggregate when there are no consecutive events having the same cell " +
    "(or overlapping in time)" in new WithSpatioTemporalSlots with WithCellCatalogue {
      events.byUserChronologically.aggTemporalOverlapAndSameCell.first._2.size should be (4)
    }

  it should "aggregate consecutive events having the same cell (or overlapping in time)" in
    new WithSpatioTemporalSlots with WithCellCatalogue {
      withSameCellEvents.byUserChronologically.aggTemporalOverlapAndSameCell.first._2.size should be(4)
    }

  it should "merge compatible slots" in new WithCompatibilitySlots with WithCellCatalogue {
    slots.combine.first._2.size should be(5)
  }

  it should "merge no slots when there is only one" in new WithCompatibilitySlots with WithCellCatalogue {
    onlySlot.combine.first._2.size should be(1)
  }

  it should "create user model entities" in new WithModelSlots {
    val model = slots.toUserCentric.first._2
    model._1.size should be (3)
    model._2.size should be (2)
    model._3.size should be (2)
  }
}
