/*
 * TODO: License goes here!
 */

package sa.com.mobily.metrics

/**
 * Metric result object. It contains a map with metrics in format [MetricResultKey,Long]
 * @param metricValue Map of [MetricKeyValue,Long]
 */
case class MetricResult(metricValue: Map[MetricResultKey, Long] = Map()) extends Serializable {

  override def toString: String = metricValue.toString

  def add(measurable: Measurable): MetricResult = {
    val m2 = SanityMetrics.applyMetrics(measurable)
    MetricResult(metricValue ++ m2.map { case (k, v) => k -> (v + metricValue.getOrElse(k, 0L)) })
  }

  def add(metric: MetricResult): MetricResult = {
    val m2 = metric.metricValue
    MetricResult(metricValue ++ m2.map { case (k, v) => k -> (v + metricValue.getOrElse(k, 0L)) })
  }
}
