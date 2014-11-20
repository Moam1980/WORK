/*
 * TODO: License goes here!
 */

package sa.com.mobily.metrics

/**
 * Trait for metric objects. We need a getKey method for processing it
 * @tparam T Subclass of Measurable
 */
trait Metric[T <: Measurable] extends Serializable {

  /**
   * Base function for generating bin for grouping objects and processing them
   * @param measurable Measurable object
   * @return The MetricKey object for grouping
   */
  def bin(measurable: T): MetricKey

  def totalFunction(measurable: Measurable): Map[MetricResultKey, Long] =
    Map(MetricResultKey("Total-items", MetricKey("all")) -> 1)
}
