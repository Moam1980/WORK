/*
 * TODO: License goes here!
 */

package sa.com.mobily.metrics

/**
 * Base trait for metrics functions
 * @tparam T Type of input for grouping by key
 */
trait MetricFunctions[T <: Measurable] extends Serializable {

  /**
   * List of functions to be applied. Must be implemented for adding functions
   */
  val functions: List[(Measurable) => Map[MetricResultKey, Long]]

  /**
   * Function for apply all metrics on an Object reference
   * @param measurable The measurable object to process
   * @return Map of K,V with metrics results
   */
  def applyMetrics(measurable: T): Map[MetricResultKey, Long] =
    functions.flatMap(metricFunction => metricFunction(measurable)).toMap
}
