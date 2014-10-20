/*
 * TODO: License goes here!
 */

package sa.com.mobily.metrics

/**
 * Trait for metrics by type of object
 */
trait MeasurableByType extends Measurable {

  def typeValue: String
}
