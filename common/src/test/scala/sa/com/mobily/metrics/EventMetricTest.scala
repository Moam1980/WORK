package sa.com.mobily.metrics

import com.github.nscala_time.time.Imports._
import org.scalatest.{FlatSpec, ShouldMatchers}

class EventMetricTest extends FlatSpec with ShouldMatchers {

  trait WithTimestampForEvents {

    val formatter = DateTimeFormat.forPattern("dd/MM/yyyy HH:mm:ss")
    val dates = Array[Long](
      toMillis("19/07/2011 16:15:07"),
      toMillis("19/07/2011 16:35:07"),
      toMillis("19/07/2011 16:35:07"),
      toMillis("19/07/2011 00:45:07"),
      toMillis("19/07/2011 01:45:07"),
      toMillis("19/07/2011 23:45:07")
    )

    def toMillis(dateAsString: String): Long = formatter.parseDateTime(dateAsString).getMillis
  }

  "EventMetric" should "return key for ending date period (30)" in new WithTimestampForEvents {
    EventMetric.groupByPeriod(dates(0), 30) should be(toMillis("19/07/2011 16:30:00"))
  }

  it should "return key for next ending date period (30)" in new WithTimestampForEvents {
    EventMetric.groupByPeriod(dates(1), 30) should be(toMillis("19/07/2011 17:00:00"))
  }

  it should "return key for ending date period (20)" in new WithTimestampForEvents {
    EventMetric.groupByPeriod(dates(2), 20) should be(toMillis("19/07/2011 16:40:00"))
  }

  it should "return key for ending date period (90)" in new WithTimestampForEvents {
    EventMetric.groupByPeriod(dates(3), 90) should be(toMillis("19/07/2011 01:30:00"))
  }

  it should "return key for next date period (90)" in new WithTimestampForEvents {
    EventMetric.groupByPeriod(dates(4), 90) should be(toMillis("19/07/2011 03:0:00"))
  }

  it should "return key for next day" in new WithTimestampForEvents {
    EventMetric.groupByPeriod(dates(5), 90) should be(toMillis("20/07/2011 0:00:00"))
  }

  it should "return a date with time 0:00:00" in new WithTimestampForEvents {
    EventMetric.resetForKey(new DateTime(dates(2))) should be(formatter.parseDateTime("19/07/2011 0:00:00"))
  }

  an[IllegalArgumentException] should be thrownBy new WithTimestampForEvents {
    EventMetric.groupByPeriod(dates(3), 13) should be(toMillis("20/07/2011 0:00:00"))
  }
}
