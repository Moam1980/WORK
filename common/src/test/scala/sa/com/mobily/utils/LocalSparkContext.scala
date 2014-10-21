/*
 * TODO: License goes here!
 */

package sa.com.mobily.utils

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.scalatest.{BeforeAndAfterAll, FlatSpec}

trait LocalSparkContext extends BeforeAndAfterAll { self: FlatSpec =>

  @transient var sc: SparkContext = _

  override def beforeAll {
    Logger.getRootLogger.setLevel(Level.ERROR)
    sc = LocalSparkContext.getNewLocalSparkContext(1, "test")
  }

  override def afterAll {
    sc.stop()
    System.clearProperty("spark.driver.port")
  }
}

object LocalSparkContext {

  def getNewLocalSparkContext(pll: Int = 1, title: String): SparkContext = new SparkContext("local[" + pll + "]", title)
}
