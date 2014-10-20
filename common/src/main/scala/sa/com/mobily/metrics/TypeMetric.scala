/*
 * TODO: License goes here!
 */

package sa.com.mobily.metrics

object TypeMetric extends Metric[MeasurableByType] {

  def metricFunction(measurable: Measurable): Map[MetricResultKey, Long] =
    Map(MetricResultKey("Items-by-type", bin(measurable.asInstanceOf[MeasurableByType])) -> 1)

  /**
   * Base function for generating key for grouping objects and processing them
   * @param measurable Measurable object
   * @return The MetricKey object for grouping
   */
  override def bin(measurable: MeasurableByType): MetricKey =
    MetricKey(measurable.asInstanceOf[MeasurableByType].typeValue)
}
