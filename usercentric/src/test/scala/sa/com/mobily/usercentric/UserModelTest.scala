/*
 * TODO: License goes here!
 */

package sa.com.mobily.usercentric

import org.scalatest.{FlatSpec, ShouldMatchers}

import sa.com.mobily.cell.{Micro, FourGFdd, Cell}
import sa.com.mobily.event.{PsEventSource, Event}
import sa.com.mobily.geometry.UtmCoordinates
import sa.com.mobily.user.User

class UserModelTest extends FlatSpec with ShouldMatchers {

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
      outSpeed = Some(0.5),
      minSpeedPointWkt = Some("POINT (1 1)"))
    val event2 = event1.copy(beginTime = 3, endTime = 4, cellId = 3, inSpeed = Some(0.5))
    val event3 = event1.copy(beginTime = 5, endTime = 6, cellId = 4, outSpeed = Some(1))
    val event4 = event1.copy(beginTime = 1, endTime = 3, cellId = 5, inSpeed = Some(1))
  }

  trait WithCellCatalogue {

    val cellPrefixWkt = "POLYGON ((0 20, 10 20, 10 30, 0 30, 0 20))"
    val cellSlot1Wkt = "POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))"
    val cellSlot2Wkt = "POLYGON ((5 0, 15 0, 15 10, 5 10, 5 0))"
    val cellSuffixWkt = "POLYGON ((20 0, 20 10, 30 10, 30 0, 20 0))"
    val slot1AfterWkt = "POLYGON ((200 200, 210 200, 210 210, 200 210, 200 200))"
    val slot2AfterWkt = "POLYGON ((205 200, 215 200, 215 210, 205 210, 205 200))"
    val suffix2Wkt = "POLYGON ((220 200, 220 210, 230 210, 230 200, 220 200))"
    val cellTimeExt1Wkt = "POLYGON ((0 0, 0 10, 10 10, 10 0, 0 0))"
    val cellTimeExt2Wkt = "POLYGON ((35010 0, 35010 10, 35020 10, 35020 0, 35010 0))"
    val cellTimeExt3Wkt = "POLYGON ((100010 0, 100010 10, 100020 10, 100020 0, 100010 0))"
    val cellTimeExt4Wkt = "POLYGON ((200900 0, 200900 10, 200910 10, 200910 0, 200900 0))"

    val cellPrefix = Cell(1, 1, UtmCoordinates(1, 4), FourGFdd, Micro, 20, 180, 45, 4, "1", cellPrefixWkt)
    val cellSlot1 = cellPrefix.copy(cellId = 2, coverageWkt = cellSlot1Wkt)
    val cellSlot2 = cellPrefix.copy(cellId = 3, coverageWkt = cellSlot2Wkt)
    val cellSuffix = cellPrefix.copy(cellId = 4, coverageWkt = cellSuffixWkt)
    val cellSlot1After = cellPrefix.copy(cellId = 5, coverageWkt = slot1AfterWkt)
    val cellSlot2After = cellPrefix.copy(cellId = 6, coverageWkt = slot2AfterWkt)
    val cellSuffix2 = cellPrefix.copy(cellId = 7, coverageWkt = suffix2Wkt)

    val cellTimeExt1 = Cell(1, 2, UtmCoordinates(0, 0), FourGFdd, Micro, 20, 180, 45, 4, "1", cellTimeExt1Wkt)
    val cellTimeExt2 =
      cellTimeExt1.copy(cellId = 2, planarCoords = UtmCoordinates(35010, 0), coverageWkt = cellTimeExt2Wkt)
    val cellTimeExt3 =
      cellTimeExt1.copy(cellId = 3, planarCoords = UtmCoordinates(100010, 0), coverageWkt = cellTimeExt3Wkt)
    val cellTimeExt4 =
      cellTimeExt1.copy(cellId = 4, planarCoords = UtmCoordinates(200900, 0), coverageWkt = cellTimeExt4Wkt)

    implicit val cellCatalogue = Map(
      (1, 1) -> cellPrefix,
      (1, 2) -> cellSlot1,
      (1, 3) -> cellSlot2,
      (1, 4) -> cellSuffix,
      (1, 5) -> cellSlot1After,
      (1, 6) -> cellSlot2After,
      (1, 7) -> cellSuffix2,
      (2, 1) -> cellTimeExt1,
      (2, 2) -> cellTimeExt2,
      (2, 3) -> cellTimeExt3,
      (2, 4) -> cellTimeExt4)
  }

  trait WithSpatioTemporalSlots extends WithEvents {

    val slot1 = SpatioTemporalSlot(
      user = User("", "", 1),
      startTime = 1,
      endTime = 2,
      cells = Set((1, 2)),
      firstEventBeginTime = 1,
      lastEventEndTime = 2,
      outMinSpeed = 0.5,
      intraMinSpeedSum = 1,
      numEvents = 3)
    val slot2 = SpatioTemporalSlot(
      user = User("", "", 1),
      startTime = 2,
      endTime = 5,
      cells = Set((1, 3)),
      firstEventBeginTime = 2,
      lastEventEndTime = 5,
      outMinSpeed = 0,
      intraMinSpeedSum = 0,
      numEvents = 1)
    val slot1WithScore = slot1.copy(score = Some(CompatibilityScore(0.5, 0, 0.5)))
    val slot1And4 = slot1.copy(
      endTime = 3,
      cells = Set((1, 2), (1, 5)),
      lastEventEndTime = 3,
      intraMinSpeedSum = 1.5,
      numEvents = 4)
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
    val dwell1 = Dwell(slot1)
    val dwell2 = Dwell(slot2)
    val dwell3 = Dwell(slot3)
    val journey1To2NoVp = Journey(
      user = User("", "", 1),
      id = 0,
      startTime = 2,
      endTime = 10,
      geomWkt = "LINESTRING (5 25, 25 5)",
      cells = Seq(),
      firstEventBeginTime = 2,
      lastEventEndTime = 10,
      numEvents = 0)
    val journey1To2 = journey1To2NoVp.copy(
      geomWkt = "LINESTRING (5 25, 5 5, 10 5, 25 5)",
      cells = Seq((1, 2), (1, 3)),
      firstEventBeginTime = 2,
      lastEventEndTime = 6,
      numEvents = 6)
    val jvp1 = JourneyViaPoint(slotJvp1.copy(typeEstimate = JourneyViaPointEstimate), 0)
    val jvp2 = JourneyViaPoint(slotJvp2.copy(typeEstimate = JourneyViaPointEstimate), 0)
  }

  trait WithModelSlotsForTimeExtension extends WithCellCatalogue {

    val startOfDay = 1423429200000L
    val endOfDay = 1423515599999L

    val dwellSlot1 = SpatioTemporalSlot(
      user = User("", "1", 1),
      startTime = 1423468095000L,
      endTime = 1423469636000L,
      cells = Set((2, 1)),
      firstEventBeginTime = 1423468095000L,
      lastEventEndTime = 1423469636000L,
      outMinSpeed = 7.5,
      intraMinSpeedSum = 0.5,
      numEvents = 4)
    val dwellSlot20 = SpatioTemporalSlot(
      user = User("", "1", 1),
      startTime = 1423471436000L,
      endTime = 1423505663000L,
      cells = Set((2, 2)),
      firstEventBeginTime = 1423471436000L,
      lastEventEndTime = 1423505663000L,
      outMinSpeed = 7.5,
      intraMinSpeedSum = 0.5,
      numEvents = 4)
    val dwellSlot21 = SpatioTemporalSlot(
      user = User("", "1", 1),
      startTime = 1423503235000L,
      endTime = 1423505663000L,
      cells = Set((2, 2)),
      firstEventBeginTime = 1423503235000L,
      lastEventEndTime = 1423505663000L,
      outMinSpeed = 7.5,
      intraMinSpeedSum = 0.5,
      numEvents = 4)
    val dwellSlot22 = SpatioTemporalSlot(
      user = User("", "1", 1),
      startTime = 1423471436000L,
      endTime = 1423505663000L,
      cells = Set((2, 3)),
      firstEventBeginTime = 1423471436000L,
      lastEventEndTime = 1423505663000L,
      outMinSpeed = 7.5,
      intraMinSpeedSum = 0.5,
      numEvents = 4)
    val dwellSlot23 = SpatioTemporalSlot(
      user = User("", "1", 1),
      startTime = 1423503235000L,
      endTime = 1423505663000L,
      cells = Set((2, 3)),
      firstEventBeginTime = 1423503235000L,
      lastEventEndTime = 1423505663000L,
      outMinSpeed = 7.5,
      intraMinSpeedSum = 0.5,
      numEvents = 4)
    val viaPoint15 = SpatioTemporalSlot(
      user = User("", "1", 1),
      startTime = 1423471436000L,
      endTime = 1423505663000L,
      cells = Set((2, 2)),
      firstEventBeginTime = 1423471436000L,
      lastEventEndTime = 1423505663000L,
      outMinSpeed = 7.5,
      intraMinSpeedSum = 0.5,
      numEvents = 4,
      typeEstimate = JourneyViaPointEstimate)
    val viaPoint16 = viaPoint15.copy(startTime = 1423489415000L, endTime = 1423489931000L, cells = Set((2, 3)))
    val viaPoint17 = viaPoint15.copy(startTime = 1423493522000L, endTime = 1423505663000L, cells = Set((2, 3)))
    val dwellSlot30 = SpatioTemporalSlot(
      user = User("", "1", 1),
      startTime = 1423507833000L,
      endTime = 1423514411000L,
      cells = Set((2, 3)),
      firstEventBeginTime = 1423507833000L,
      lastEventEndTime = 1423514411000L,
      outMinSpeed = 7.5,
      intraMinSpeedSum = 0.5,
      numEvents = 4)
    val dwellSlot31 = dwellSlot30.copy(cells = Set((2, 4)))
  }

  trait WithModelSlotsForViaPointDetection {

    val dwellSlot1 = SpatioTemporalSlot(
      user = User("", "1", 1),
      startTime = 0L,
      endTime = 1000L,
      cells = Set((2, 1)),
      firstEventBeginTime = 0L,
      lastEventEndTime = 1000L,
      outMinSpeed = 0.5,
      intraMinSpeedSum = 0.5,
      numEvents = 4)
    val dwellSlot2 = dwellSlot1.copy(
      startTime = 1000L,
      endTime = 301000L,
      firstEventBeginTime = 1000L,
      lastEventEndTime = 301000L)
    val dwellSlot3 = dwellSlot1.copy(
      startTime = 1000000L,
      endTime = 1900000L,
      firstEventBeginTime = 1000000L,
      lastEventEndTime = 1900000L)
    val dwellSlot4 = dwellSlot1.copy(
      startTime = 1905000L,
      endTime = 1995000L,
      firstEventBeginTime = 1905000L,
      lastEventEndTime = 1995000L)
    val dwellSlot5 = dwellSlot1.copy(
      startTime = 2000000L,
      endTime = 2200000L,
      firstEventBeginTime = 2000000L,
      lastEventEndTime = 2200000L)
  }

  "UserModel" should "return an empty list with no events when aggregating same cells" in
    new WithSpatioTemporalSlots with WithCellCatalogue {
      UserModel.aggTemporalOverlapAndSameCell(List()) should be(List())
    }

  it should "return a single spatio-temporal item with a list with a single element when aggregating same cells" in
    new WithSpatioTemporalSlots with WithCellCatalogue {
      UserModel.aggTemporalOverlapAndSameCell(List(event1)) should be(List(SpatioTemporalSlot(event1)))
    }

  it should "return different spatio-temporal items with a list with no events with the same cell " +
    "when aggregating same cells" in new WithSpatioTemporalSlots with WithCellCatalogue {

    UserModel.aggTemporalOverlapAndSameCell(List(event1, event2, event3)) should
      be(List(SpatioTemporalSlot(event1), SpatioTemporalSlot(event2), SpatioTemporalSlot(event3)))
  }

  it should "aggregate consecutive events having the same cell" in
    new WithSpatioTemporalSlots with WithCellCatalogue {
      UserModel.aggTemporalOverlapAndSameCell(List(event1, event1, event1, event2)) should
        be(List(slot1, SpatioTemporalSlot(event2)))
    }

  it should "aggregate consecutive events overlapping in time" in
    new WithSpatioTemporalSlots with WithCellCatalogue {
      UserModel.aggTemporalOverlapAndSameCell(List(event1, event1, event4, event1)) should be(List(slot1And4))
    }

  it should "do nothing when computing scores for an empty list" in new WithSpatioTemporalSlots with WithCellCatalogue {
    UserModel.computeScores(List()) should be(List())
  }

  it should "not compute any score for a list with a single element" in
    new WithSpatioTemporalSlots with WithCellCatalogue {
      UserModel.computeScores(List(slot1)) should be(List(slot1))
    }

  it should "compute scores for all elements but the last one in a list with two or more elements" in
    new WithSpatioTemporalSlots with WithCellCatalogue {
      UserModel.computeScores(List(slot1, slot2)) should be(List(slot1WithScore, slot2))
    }

  it should "not merge any slot when the max score is None (single element)" in
    new WithCompatibilitySlots with WithCellCatalogue {
      UserModel.aggregateCompatible(List(suffixSlot)) should be(List(suffixSlot))
    }

  it should "not merge any slot when the max score is zero" in new WithCompatibilitySlots with WithCellCatalogue {
    UserModel.aggregateCompatible(List(prefixSlot, suffixSlot)) should be(List(prefixSlot, suffixSlot))
  }

  it should "merge slots when they are in between other slots (and recompute scores)" in
    new WithCompatibilitySlots with WithCellCatalogue {
      UserModel.aggregateCompatible(List(prefixSlot, slot1, slot2, suffixSlot)) should
        be(List(prefixSlot, mergedSlot, suffixSlot))
    }

  it should "merge slots when there are no slots before (and recompute scores)" in
    new WithCompatibilitySlots with WithCellCatalogue {
      UserModel.aggregateCompatible(List(slot1, slot2, suffixSlot)) should be(List(mergedSlot, suffixSlot))
    }

  it should "merge slots when there are no slots after (and recompute scores)" in
    new WithCompatibilitySlots with WithCellCatalogue {
      UserModel.aggregateCompatible(List(prefixSlot, slot1, slot2)) should
        be(List(prefixSlot, mergedSlot.copy(score = None)))
    }

  it should "merge slots when there are no slots before and after" in
    new WithCompatibilitySlots with WithCellCatalogue {
      UserModel.aggregateCompatible(List(slot1, slot2)) should be(List(mergedSlot.copy(score = None)))
    }

  it should "merge slots when there are several pairs with the same score (and recompute scores)" in
    new WithCompatibilitySlots with WithCellCatalogue {
      UserModel.aggregateCompatible(
        List(prefixSlot, slot1, slot2, suffixSlot, slot1After, slot2After, suffixSlot2)) should
        be(List(prefixSlot, mergedSlot, suffixSlot.copy(score = Some(CompatibilityScore(0, 0, 0))), merged2Slot,
          suffixSlot2))
    }

  it should "accept an empty list of slots when filling via points" in new WithModelSlots {
    UserModel.fillViaPoints(List()) should be(List())
  }

  it should "leave unprocessed a single-element list of slots when filling via points" in new WithModelSlots {
    UserModel.fillViaPoints(List(slot1)) should be(List(slot1))
  }

  it should "not set to via points slots with a low minimum speed" in new WithModelSlots {
    UserModel.fillViaPoints(List(slot1, slot2, slot3)) should be(List(slot1, slot2, slot3))
  }

  it should "set to via points slots following a high minimum speed (single slot)" in new WithModelSlots {
    UserModel.fillViaPoints(List(slot1, slotJvp1, slot2)) should
      be(List(slot1, slotJvp1.copy(typeEstimate = JourneyViaPointEstimate), slot2))
  }

  it should "set to via points slots following a high minimum speed (several slots)" in new WithModelSlots {
    UserModel.fillViaPoints(List(slot1, slotJvp1, slotJvp2, slot2)) should
      be(List(
        slot1,
        slotJvp1.copy(typeEstimate = JourneyViaPointEstimate),
        slotJvp2.copy(typeEstimate = JourneyViaPointEstimate),
        slot2))
  }

  it should "accept an empty list of slots when building model" in new WithModelSlots {
    UserModel.userCentric(List()) should be((List(), List(), List()))
  }

  it should "treat as Dwell a list with a single slot" in new WithModelSlots {
    UserModel.userCentric(List(slot1)) should be((List(dwell1), List(), List()))
  }

  it should "build intermediate journeys with no via points" in new WithModelSlots {
    UserModel.userCentric(List(slot1, slot2)) should be((List(dwell1, dwell2), List(journey1To2NoVp), List()))
  }

  it should "build intermediate journeys with via points" in new WithModelSlots {
    UserModel.userCentric(List(
      slot1,
      slotJvp1.copy(typeEstimate = JourneyViaPointEstimate),
      slotJvp2.copy(typeEstimate = JourneyViaPointEstimate),
      slot2)) should be((List(dwell1, dwell2), List(journey1To2), List(jvp1, jvp2)))
  }

  it should "compute journey duration (short distance)" in {
    val distanceLowerThanThreshold = 30000
    UserModel.journeyDuration(distanceLowerThanThreshold) should be(2160)
  }

  it should "compute journey duration (long distance)" in {
    val distanceHigherThanThreshold = 130000
    UserModel.journeyDuration(distanceHigherThanThreshold) should be(5200)
  }

  it should "accept an empty list of slots when extending time of slots" in new WithModelSlotsForTimeExtension {
    UserModel.extendTime(List()) should be(List())
  }

  it should "extend time from beginning to end of day for a single slot" in new WithModelSlotsForTimeExtension {
    UserModel.extendTime(List(dwellSlot1)) should be(List(dwellSlot1.copy(startTime = startOfDay, endTime = endOfDay)))
  }

  it should "extend time for dwells without via points (short distance, short time gap)" in
    new WithModelSlotsForTimeExtension {
      UserModel.extendTime(List(dwellSlot1, dwellSlot20)) should
        be(List(dwellSlot1.copy(startTime = startOfDay), dwellSlot20.copy(endTime = endOfDay)))
    }

  it should "extend time for dwells without via points (short distance, long time gap)" in
    new WithModelSlotsForTimeExtension {
      val halfTimeShift = 15539000
      UserModel.extendTime(List(dwellSlot1, dwellSlot21)) should
        be(List(
          dwellSlot1.copy(startTime = startOfDay, endTime = dwellSlot1.endTime + halfTimeShift),
          dwellSlot21.copy(startTime = dwellSlot21.startTime - halfTimeShift, endTime = endOfDay)))
    }

  it should "extend time for dwells without via points (long distance, short time gap)" in
    new WithModelSlotsForTimeExtension {
      UserModel.extendTime(List(dwellSlot1, dwellSlot22)) should
        be(List(dwellSlot1.copy(startTime = startOfDay), dwellSlot22.copy(endTime = endOfDay)))
    }

  it should "extend time for dwells without via points (long distance, long time gap)" in
    new WithModelSlotsForTimeExtension {
      val halfTimeShift = 14799000
      UserModel.extendTime(List(dwellSlot1, dwellSlot23)) should
        be(List(
          dwellSlot1.copy(startTime = startOfDay, endTime = dwellSlot1.endTime + halfTimeShift),
          dwellSlot23.copy(startTime = dwellSlot23.startTime - halfTimeShift, endTime = endOfDay)))
    }

  it should "extend time for dwells followed/following via point (short time gap)" in
    new WithModelSlotsForTimeExtension {
      UserModel.extendTime(List(dwellSlot1, viaPoint15, dwellSlot30)) should
        be(List(dwellSlot1.copy(startTime = startOfDay), viaPoint15, dwellSlot30.copy(endTime = endOfDay)))
    }

  it should "extend time for dwells followed/following via point (long time gap)" in
    new WithModelSlotsForTimeExtension {
      UserModel.extendTime(List(dwellSlot1, viaPoint16, dwellSlot31)) should
        be(List(
          dwellSlot1.copy(startTime = startOfDay, endTime = viaPoint16.startTime - 4000000),
          viaPoint16,
          dwellSlot31.copy(startTime = viaPoint16.endTime + 4036000, endTime = endOfDay)))
  }

  it should "not extend time for two consecutive via points" in new WithModelSlotsForTimeExtension {
    UserModel.extendTime(List(dwellSlot1, viaPoint16, viaPoint17, dwellSlot31)) should
      be(List(
        dwellSlot1.copy(startTime = startOfDay, endTime = viaPoint16.startTime - 4000000),
        viaPoint16,
        viaPoint17,
        dwellSlot31.copy(endTime = endOfDay)))
  }

  it should "accept an empty list of slots when detecting possible via points" in {
    UserModel.candidatesToViaPoints(List()) should be(List())
  }

  it should "not convert dwell slots when there are just one slot" in new WithModelSlotsForViaPointDetection {
    UserModel.candidatesToViaPoints((List(dwellSlot1))) should be (List(dwellSlot1))
  }

  it should "not convert dwell slots when there are just two slots" in new WithModelSlotsForViaPointDetection {
    UserModel.candidatesToViaPoints((List(dwellSlot1, dwellSlot2))) should be (List(dwellSlot1, dwellSlot2))
  }

  it should "convert suitable dwell slots to via points (but first and last ones)" in
    new WithModelSlotsForViaPointDetection {
      UserModel.candidatesToViaPoints(List(dwellSlot1, dwellSlot2, dwellSlot3, dwellSlot4, dwellSlot5)) should
        be (List(
          dwellSlot1,
          dwellSlot2.copy(typeEstimate = JourneyViaPointEstimate),
          dwellSlot3,
          dwellSlot4.copy(typeEstimate = JourneyViaPointEstimate),
          dwellSlot5))
    }

  it should "identify probable via points" in new WithModelSlotsForViaPointDetection {
    UserModel.probableViaPoint(dwellSlot1) should be (true)
    UserModel.probableViaPoint(dwellSlot2) should be (true)
    UserModel.probableViaPoint(dwellSlot3) should be (false)
  }
}
