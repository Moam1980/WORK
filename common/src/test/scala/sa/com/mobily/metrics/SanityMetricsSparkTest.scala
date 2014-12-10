package sa.com.mobily.metrics

import com.github.nscala_time.time.Imports._
import org.scalatest.{FlatSpec, ShouldMatchers}

import sa.com.mobily.event.Event
import sa.com.mobily.event.spark.EventDsl
import sa.com.mobily.user.User
import sa.com.mobily.utils.{EdmCoreUtils, LocalSparkContext}

class SanityMetricsSparkTest extends FlatSpec with ShouldMatchers with LocalSparkContext {

  import EventDsl._

  def toMillis(dateAsString: String): Long = DateTimeFormat.forPattern("dd/MM/yyyy HH:mm:ss")
    .withZone(EdmCoreUtils.TimeZoneSaudiArabia).parseDateTime(dateAsString).getMillis

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
    val event2 = event0.copy(beginTime = toMillis("19/07/2011 00:45:07"), eventType = "1",
      user = User(
        imei = "866173010386736",
        imsi = "420034122616618",
        msisdn = 330946079L))
    val event3 = event0.copy(beginTime = toMillis("19/07/2011 01:45:07"), eventType = "3")
    val event4 = event0.copy(beginTime = toMillis("19/07/2011 19:45:07"))
    val key0 = MetricResultKey("Total-number-items", MetricKey(1311080400000L))
    val key4 = MetricResultKey("Items-by-type", MetricKey("859"))
    val key5 = MetricResultKey("Items-by-type", MetricKey("1"))
    val key6 = MetricResultKey("Items-by-type", MetricKey("3"))
    val falseKey = MetricResultKey("Total-number-items", MetricKey(1311028212000L))
    val events = sc.parallelize(List(event0, event1, event2, event3, event4))
    implicit val accumulableParam = new MetricResultParam[Measurable]()
    val accumulable = sc.accumulable(MetricResult(), "sanity")
  }

  "TimeMetric" should "process a list of Events and group it" in new WithEventsForMetrics {
    val metrics: MetricResult = events.metrics
    metrics.metricValue.get(key0) should be(Some(1))
    metrics.metricValue.get(falseKey) should be(None)
    metrics.metricValue.get(key4) should be(Some(2))
    metrics.metricValue.get(key5) should be(Some(2))
    metrics.metricValue.get(key6) should be(Some(1))
  }
}
