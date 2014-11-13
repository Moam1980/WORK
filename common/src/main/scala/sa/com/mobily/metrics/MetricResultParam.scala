/*
 * TODO: License goes here!
 */

package sa.com.mobily.metrics

import org.apache.spark.AccumulableParam

class MetricResultParam[T <: Measurable] extends AccumulableParam[MetricResult, T] {

  override def addAccumulator(result: MetricResult, measurable: T): MetricResult = result.add(measurable)

  override def addInPlace(r1: MetricResult, r2: MetricResult): MetricResult = r1.add(r2)

  override def zero(initialValue: MetricResult): MetricResult = MetricResult()
}
