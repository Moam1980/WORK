/*
 * TODO: License goes here!
 */

package sa.com.mobily.metrics

/**
 * Object that contains the list of functions to be applied for sanity
 */
object SanityMetrics extends MetricFunctions[Measurable] {

  override val functions = List[(Measurable) =>
    Map[MetricResultKey, Long]](
      TypeMetric.metricFunction,
      TypeMetric.totalFunction,
      TemporalMetric.metricFunction)
}
