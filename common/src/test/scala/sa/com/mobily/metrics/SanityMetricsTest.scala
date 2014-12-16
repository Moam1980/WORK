package sa.com.mobily.metrics

import com.github.nscala_time.time.Imports._
import org.apache.spark.Accumulable
import org.scalatest.{FlatSpec, ShouldMatchers}

import sa.com.mobily.event.Event
import sa.com.mobily.user.User
import sa.com.mobily.utils.{EdmCoreUtils, LocalSparkContext}

class SanityMetricsTest extends FlatSpec with ShouldMatchers with LocalSparkContext {

  trait WithEventsForMetrics {

    val event0 = Event(
      user = User(
        imei = "866173010386736",
        imsi = "420034122616618",
        msisdn = 560917079L),
      beginTime = toMillis("19/07/2011 16:15:07"),
      endTime = 1404162610000L,
      lacTac = 0x052C,
      cellId = 13067,
      eventType = "859",
      subsequentLacTac = None,
      subsequentCellId = None)
    val event1 = event0.copy(beginTime = toMillis("19/07/2011 16:35:07"), eventType = "1")
    val event2 = event0.copy(beginTime = toMillis("19/07/2011 00:45:07"), eventType = "1")
    val event3 = event0.copy(beginTime = toMillis("19/07/2011 01:45:07"), eventType = "3")
    val event4 = event0.copy(beginTime = toMillis("19/07/2011 19:45:07"))
    val key0 = MetricResultKey("Total-number-items", MetricKey(1311028200000L))
    val key1 = MetricResultKey("Total-number-items", MetricKey(1311028212000L))
    val key2 = MetricResultKey("Total-number-items", MetricKey(1311080400000L))
    val key3 = MetricResultKey("Total-number-items", MetricKey(1311024600000L))
    val key4 = MetricResultKey("Items-by-type", MetricKey("859"))
    val key5 = MetricResultKey("Items-by-type", MetricKey("1"))
    val key6 = MetricResultKey("Items-by-type", MetricKey("3"))
    val falseKey = MetricResultKey("Total-number-items", MetricKey(1311028212000L))

    def toMillis(dateAsString: String): Long = DateTimeFormat.forPattern("dd/MM/yyyy HH:mm:ss")
      .withZone(EdmCoreUtils.TimeZoneSaudiArabia).parseDateTime(dateAsString).getMillis
  }

  trait WithEventsForSpark extends WithEventsForMetrics {

    val events = sc.parallelize(List(event0, event1, event2, event3, event4))
    implicit val accumulableParam = new MetricResultParam[Measurable]()
    val accumulable: Accumulable[MetricResult, Measurable] = sc.accumulable(MetricResult(), "sanity")
  }

  "TimeMetric" should "should work for different time chunks" in new WithEventsForMetrics {
    TemporalMetric.bin(event0).bin should be(toMillis("19/07/2011 16:00:00"))
    TemporalMetric.bin(event1).bin should be(toMillis("19/07/2011 16:30:00"))
    TemporalMetric.bin(event1, 20).bin should be(toMillis("19/07/2011 16:20:00"))
    TemporalMetric.bin(event2, 90).bin should be(toMillis("19/07/2011 00:00:00"))
    TemporalMetric.bin(event3, 90).bin should be(toMillis("19/07/2011 01:30:00"))
  }

  it should "return a date with time 0:00:00" in new WithEventsForMetrics {
    TemporalMetric.resetForKey(event0.beginTime) should
      be(new DateTime(toMillis("19/07/2011 00:00:00"), EdmCoreUtils.TimeZoneSaudiArabia))
  }

  an[IllegalArgumentException] should be thrownBy new WithEventsForMetrics {
    TemporalMetric.bin(event0, 13)
  }

  it should "process a list of Events and grouping it" in new WithEventsForSpark {
    events.collect.map(event => accumulable += event)
    accumulable.value.metricValue.get(key0) should be(Some(1))
    accumulable.value.metricValue.get(falseKey) should be(None)
    accumulable.value.metricValue.get(key4) should be(Some(2))
    accumulable.value.metricValue.get(key5) should be(Some(2))
    accumulable.value.metricValue.get(key6) should be(Some(1))
  }

  it should "check the equality of objects" in new WithEventsForMetrics {
    key0.canEqual(event1) should be(false)
    MetricResultKey("Total-number-items", MetricKey(1311028200000L, Some(MetricKey(859)))).toString should
      be("Total-number-items: 1311028200000-859")
  }

  it should "return fields on CSV format" in new WithEventsForMetrics {
    key0.canEqual(event1) should be(false)
    MetricResultKey("Total-number-items", MetricKey(1311028200000L, Some(MetricKey(859)))).toString should
      be("Total-number-items: 1311028200000-859")
  }
}
