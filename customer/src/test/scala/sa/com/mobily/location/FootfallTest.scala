/*
 * TODO: License goes here!
 */

package sa.com.mobily.location

import org.scalatest._

import sa.com.mobily.user.User
import sa.com.mobily.usercentric.Dwell

class FootfallTest extends FlatSpec with ShouldMatchers {

  trait WithFootfalls {

    val user1 = User(imei = "", imsi = "420031", msisdn = 9661)
    val user2 = User(imei = "", imsi = "420032", msisdn = 9662)
    val user3 = User(imei = "", imsi = "420033", msisdn = 9663)
    val footfall = Footfall(users = Set(user1, user2, user3), numDwells = 7, avgPrecision = 10000)
    
    val dwell = Dwell(
      user = user1,
      startTime = 1,
      endTime = 10,
      geomWkt = "POLYGON ((0 0, 0 10, 10 10, 10 0, 0 0))",
      cells = Seq((2, 4), (2, 6)),
      firstEventBeginTime = 3,
      lastEventEndTime = 9,
      numEvents = 2)
    val dwellFootfall = Footfall(users = Set(user1), numDwells = 1, avgPrecision = 100)

    val aggFootfall = Footfall(users = Set(user1, user2, user3), numDwells = 8, avgPrecision = 8762.5)
  }

  it should "compute footfall" in new WithFootfalls {
    footfall.numDistinctUsers should be (3)
  }

  it should "return header" in new WithFootfalls {
    Footfall.Header should be (Array("footfall", "numDwells", "avgPrecision"))
  }

  it should "return fields" in new WithFootfalls {
    footfall.fields should be (Array("3", "7", "10000.0"))
  }

  it should "have the same number of fields in header and fields returned" in new WithFootfalls {
    footfall.fields.size should be (Footfall.Header.size)
  }

  it should "build from Dwell" in new WithFootfalls {
    Footfall(dwell) should be (dwellFootfall)
  }

  it should "aggregate footfalls" in new WithFootfalls {
    Footfall.aggregate(footfall, dwellFootfall) should be (aggFootfall)
  }
}
