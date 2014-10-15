package sa.com.mobily.utils

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterEach, FunSuite}
import sa.com.mobily.event.{CsEvent, Event}

/**
 * Created by rsaez on 13/10/14.
 */
class AvroParquetRDDUtils$Test extends FunSuite with BeforeAndAfterEach {

  val csvFile: String = "file:///home/rsaez/tmp/kk.csv"

  @transient var sqc: SQLContext = _

  override def beforeEach() {
    println("creating spark context")
    val conf =
      new SparkConf(false)
        //.setMaster("spark://172.19.0.215:7077")
        .setMaster("local[8]")
        .setAppName("test")
    sqc = new SQLContext(new SparkContext(conf))

  }

  private def resetSparkContext(): Unit = {

    if (sqc != null) {
      println("stopping Spark Context")
      sqc.sparkContext.stop()
    }
    sqc = null

  }

  override def afterEach() {
    resetSparkContext()
  }

  test("Read from CSV and Write to Parquet") {

    val map: RDD[CsEvent] = sqc.sparkContext.textFile(csvFile).map(line => {
      Event.fromCsv.fromFields(line.split(","))
    })

    // for write to parquet in local
    sqc.createSchemaRDD(map).saveAsParquetFile("file:///tmp/parquet" + System.currentTimeMillis())

    // for write to parquet in HDFS
    // sqc.createSchemaRDD(map).saveAsParquetFile("hdfs://10.200.0.49:8020/user/rsaez/parquet" + System.currentTimeMillis())

    // for write to avro + parquet in local
    AvroParquetRDDUtils.writeParquetRDD(sqc.sparkContext, map.map[(Void, CsEvent)](event => (null, event)), Event.SCHEMA$, "file:///tmp/avro_parquet" + System.currentTimeMillis())

    // for write to avro + parquet in HDFS
    // AvroParquetRDDUtils.writeParquetRDD(sqc.sparkContext, map.map[(Void, CsEvent)](event => (null, event)), Event.SCHEMA$, "hdfs://10.200.0.49:8020/user/rsaez/avro_parquet" + System.currentTimeMillis())

  }

}
