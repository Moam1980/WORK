/*
 * TODO: License goes here!
 */

package sa.com.mobily.metrics

import com.github.nscala_time.time.Imports._

import sa.com.mobily.utils.EdmCoreUtils

object TemporalMetric extends Metric[MeasurableByTime] {

  val HoursPerDay = 24
  val MinutesPerHour = 60
  val defaultChunkInMinutes = 30

  def metricFunction(measurable: Measurable): Map[MetricResultKey, Long] =
    Map(MetricResultKey("Total-number-items", bin(measurable.asInstanceOf[MeasurableByTime])) -> 1)

  /**
   * Base function for generating key for grouping objects and processing them
   * @param measurable Measurable object
   * @return The Metrickey object for grouping
   */
  override def bin(measurable: MeasurableByTime): MetricKey = bin(measurable, defaultChunkInMinutes)

  /**
   * This method returns a timestamp as bin for grouping dates. If it receives 2008-03-09 16:05:07.123 and 30 minutes
   * as arguments, its returns a timestamp with equivalent to 2008-03-09 16:00:00.000
   * @param measurable The MeasurableByTime object to evaluate
   * @return The timestamp resultant
   */
  def bin(measurable: MeasurableByTime, minutes: Int): MetricKey = {
    val timestamp = measurable.timeValue
    require(timestamp > 0 && minutes > 0 && MinutesPerHour * HoursPerDay % minutes == 0)
    val segment = new DateTime(timestamp, EdmCoreUtils.TimeZoneSaudiArabia).getMinuteOfDay / minutes
    MetricKey(resetForKey(timestamp).plusMinutes(minutes * segment).getMillis)
  }

  /**
   * Timestamp reset to 0:00:00.000
   * @return Date with time sets to 0:00:00.000
   */
  def resetForKey(timestamp: Long): DateTime =
    new DateTime(timestamp, EdmCoreUtils.TimeZoneSaudiArabia)
      .withHour(0).withMinute(0).withSecond(0).withMillisOfSecond(0)
}
