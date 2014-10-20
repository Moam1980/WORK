/*
 * TODO: License goes here!
 */

package sa.com.mobily.metrics

/**
 * Class for encapsulate the metric key logic.
 * @param keyName The name of the metric
 * @param metricKey The key object of the metric
 */
case class MetricResultKey(keyName: String, metricKey: MetricKey) extends Serializable {

  override def toString: String = s"$keyName: $metricKey"
}
