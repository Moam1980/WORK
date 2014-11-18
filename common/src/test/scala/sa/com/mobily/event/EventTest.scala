/*
 * TODO: License goes here!
 */

package sa.com.mobily.event

import org.apache.spark.sql.catalyst.expressions.Row
import org.scalatest.{FlatSpec, ShouldMatchers}

import sa.com.mobily.user.User

class EventTest extends FlatSpec with ShouldMatchers {

  import Event._

  trait WithEvent {

    val event = Event(
      User(imei = "866173010386736", imsi = "420034122616618", msisdn = 560917079L),
      beginTime = 1404162126000L,
      endTime = 1404162610000L,
      lacTac = 0x052C,
      cellId = 13067,
      eventType = "859",
      subsequentLacTac = Some(0),
      subsequentCellId = Some(0))
    val row = Row(Row("866173010386736", "420034122616618", 560917079L),
      1404162126000L, 1404162610000L, 0x052C, 13067, "859", 0, 0, None, None, None)
    val wrongRow = Row(Row(866173010386L, "420034122616618", 560917079L),
      1404162126000L, 1404162610000L, 0x052C, 13067, "859", 0, 0, None, None, None)
  }

  "Event" should "prefer LAC to TAC" in {
    Event.lacOrTac("1", "2") should be ("1")
  }

  it should "return TAC when LAC is missing" in {
    Event.lacOrTac("", "2") should be ("2")
  }

  it should "prefer SAC to CI" in {
    Event.sacOrCi("1", "2") should be ("1")
  }

  it should "return CI when SAC is missing" in {
    Event.sacOrCi("", "2") should be ("2")
  }

  it should "be built from Row with a Event" in new WithEvent {
    fromRow.fromRow(row) should be (event)
  }

  it should "be discarded when row is wrong" in new WithEvent {
    an[Exception] should be thrownBy fromRow.fromRow(wrongRow)
  }
}
