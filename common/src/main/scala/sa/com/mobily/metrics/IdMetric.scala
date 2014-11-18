/*
 * TODO: License goes here!
 */

package sa.com.mobily.metrics

object IdMetric extends Metric[MeasurableById[Any]] {

  def metricFunction(measurable: Measurable): Map[MetricResultKey, Long] =
    Map(MetricResultKey("Items-by-ID", bin(measurable.asInstanceOf[MeasurableById[Any]])) -> 1)

  /**
   * Base function for generating bin for grouping objects and processing them
   * @param measurable Measurable object
   * @return The MetricKey object for grouping
   */
  override def bin(measurable: MeasurableById[Any]): MetricKey =
    MetricKey(measurable.asInstanceOf[MeasurableById[Any]].id)
}
