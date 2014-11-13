/*
 * TODO: License goes here!
 */

package sa.com.mobily.metrics

/**
 * Trait for temporal metrics
 */
trait MeasurableByTime extends Measurable {

  def timeValue: Long
}
