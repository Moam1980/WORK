/*
 * TODO: License goes here!
 */

package sa.com.mobily.metrics

/**
 * Case class for key metrics. We can nest more keys for complex metrics
 * @param bin The bin of the metric
 * @param nestedKey The key of the metric
 */
case class MetricKey(bin: Any, nestedKey: Option[MetricKey] = None) extends Serializable {

  override def toString: String = bin + nestedKey.map("-" + _.toString).getOrElse("")
}
