/*
 * TODO: License goes here!
 */

package sa.com.mobily.metrics

import com.github.nscala_time.time.Imports._

object EventMetric {

  private val HoursPerDay = 24
  private val MinutesPerHour = 60

  /**
   * This method returns a timestamp as key for grouping dates. If it receives 2008-03-09 16:05:07.123 and 30 minutes
   * as arguments, its returns a timestamp with equivalent to 2008-03-09 16:30:00.000
   * @param timestamp The timestamp to evaluate
   * @param minutes Minutes to split keys.
   * @return The timestamp resultant
   */
  def groupByPeriod(timestamp: Long, minutes: Int): Long =
    if (timestamp > 0 && minutes > 0 && MinutesPerHour * HoursPerDay % minutes == 0) {
      val segment = 1 + new DateTime(timestamp).getMinuteOfDay / minutes
      resetForKey(new DateTime(timestamp)).plusMinutes(minutes * segment).getMillis
    } else throw new IllegalArgumentException

  /**
   * DateTime reset to 0:00:00.000
   * @param dateForReset The DateTime for reset
   * @return Date with time sets to 0:00:00.000
   */
  def resetForKey(dateForReset: DateTime): DateTime =
    dateForReset.withHour(0).withMinute(0).withSecond(0).withMillisOfSecond(0)
}
