package sa.com.mobily.utils

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterEach, FunSuite}
import sa.com.mobily.event.{PsEvent, CsEvent, Event}

/**
 * Created by rsaez on 13/10/14.
 */
class AvroParquetRDDUtils$Test extends FunSuite with BeforeAndAfterEach {

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


  test("Read CsEvents from CSV and Write to Parquet") {

    val csvFile: String = "file:///path/cs_events.csv"

    val map: RDD[CsEvent] = sqc.sparkContext.textFile(csvFile).map(line => {
      CsEvent.fromCsv.fromFields(line.split(","))
    })
    // for write to parquet in local
    sqc.createSchemaRDD(map).saveAsParquetFile("file:///tmp/parquet" + System.currentTimeMillis())

    // for write to parquet in HDFS
    // sqc.createSchemaRDD(map).saveAsParquetFile("hdfs://host_hdfs:8020/user/root/parquet" + System.currentTimeMillis())

    // for write to avro + parquet in local
    AvroParquetRDDUtils.writeParquetRDD(sqc.sparkContext, map.map[(Void, CsEvent)](event => (null, event)), CsEvent.SCHEMA$, "file:///tmp/avro_parquet" + System.currentTimeMillis())

    // for write to avro + parquet in HDFS
    // AvroParquetRDDUtils.writeParquetRDD(sqc.sparkContext, map.map[(Void, CsEvent)](event => (null, event)), Event.SCHEMA$, "hdfs://host_hdfs:8020/user/root/avro_parquet" + System.currentTimeMillis())

  }

  test("Read PsEvents from CSV and Write to Parquet") {

    val csvFile: String = "file:///path/ps_events.csv"

    val map: RDD[PsEvent] = sqc.sparkContext.textFile(csvFile).map(line => {
      PsEvent.fromCsv.fromFields(line.split(","))
    })
    // for write to parquet in local
    sqc.createSchemaRDD(map).saveAsParquetFile("file:///tmp/parquet" + System.currentTimeMillis())

    // for write to parquet in HDFS
    // sqc.createSchemaRDD(map).saveAsParquetFile("hdfs://host_hdfs:8020/user/root/parquet" + System.currentTimeMillis())

    // for write to avro + parquet in local
    AvroParquetRDDUtils.writeParquetRDD(sqc.sparkContext, map.map[(Void, PsEvent)](event => (null, event)), PsEvent.SCHEMA$, "file:///tmp/avro_parquet" + System.currentTimeMillis())

    // for write to avro + parquet in HDFS
    // AvroParquetRDDUtils.writeParquetRDD(sqc.sparkContext, map.map[(Void, CsEvent)](event => (null, event)), Event.SCHEMA$, "hdfs://host_hdfs:8020/user/root/avro_parquet" + System.currentTimeMillis())

  }

  test("Join CsEvents with PsEvents and write to Parquet") {

/*    val csCsvFile: String = "file:///path/cs_events.csv"
    val psCsvFile: String = "file:///path/ps_events.csv"

    val map: RDD[PsEvent] = sqc.sparkContext.textFile(csvFile).map(line => {
      PsEvent.fromCsv.fromFields(line.split(","))
    })
    // for write to parquet in local
    sqc.createSchemaRDD(map).saveAsParquetFile("file:///tmp/parquet" + System.currentTimeMillis())

    // for write to parquet in HDFS
    // sqc.createSchemaRDD(map).saveAsParquetFile("hdfs://host_hdfs:8020/user/root/parquet" + System.currentTimeMillis())

    // for write to avro + parquet in local
    AvroParquetRDDUtils.writeParquetRDD(sqc.sparkContext, map.map[(Void, PsEvent)](event => (null, event)), PsEvent.SCHEMA$, "file:///tmp/avro_parquet" + System.currentTimeMillis())

    // for write to avro + parquet in HDFS
    // AvroParquetRDDUtils.writeParquetRDD(sqc.sparkContext, map.map[(Void, CsEvent)](event => (null, event)), Event.SCHEMA$, "hdfs://host_hdfs:8020/user/root/avro_parquet" + System.currentTimeMillis())
*/
  }

}
