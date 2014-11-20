/*
 * TODO: License goes here!
 */

package sa.com.mobily.metrics

/**
 * Trait for metrics by unique ID
 */
trait MeasurableById[T] extends Measurable {

  def id: T
}
