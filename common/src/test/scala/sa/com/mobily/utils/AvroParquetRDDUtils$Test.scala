package sa.com.mobily.utils

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterEach, FunSuite}
import sa.com.mobily.event.{CsEvent, Event, PsEvent}

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

  /*  test("Read CsEvents from CSV and Write to Parquet") {

      val csvFile: String = "file:///home/rsaez/tmp/kk.csv"

      val map: RDD[CsEvent] = sqc.sparkContext.textFile(csvFile).map(line => {
        CsEvent.fromCsv.fromFields(line.split(","))
      })
      // for write to parquet in local
      sqc.createSchemaRDD(map).saveAsParquetFile("file:///tmp/parquet" + System.currentTimeMillis())

      // for write to avro + parquet in local
      AvroParquetRDDUtils.writeParquetRDD(sqc.sparkContext, map.map[(Void, CsEvent)](event => (null, event)), CsEvent.SCHEMA$, "file:///tmp/avro_parquet" + System.currentTimeMillis())

    }


    test("Read PsEvents from CSV and Write to Parquet") {

      val csvFile: String = "file:///home/rsaez/tmp/kk.csv"

      val map: RDD[PsEvent] = sqc.sparkContext.textFile(csvFile).map(line => {
        PsEvent.fromCsv.fromFields(line.split(","))
      })
      // for write to parquet in local
      sqc.createSchemaRDD(map).saveAsParquetFile("file:///tmp/parquet" + System.currentTimeMillis())

      // for write to avro + parquet in local
      AvroParquetRDDUtils.writeParquetRDD(sqc.sparkContext, map.map[(Void, PsEvent)](event => (null, event)), PsEvent.SCHEMA$, "file:///tmp/avro_parquet" + System.currentTimeMillis())

    }*/

  test("Join CsEvents with PsEvents and write to Parquet") {

    import sa.com.mobily.event.spark.EventContext._

    val csCsvFile: String = "file:///home/rsaez/tmp/cs_events.csv"
    val psCsvFile: String = "file:///home/rsaez/tmp/ps_events.csv"

    val csEvents: RDD[CsEvent] = sqc.sparkContext.textFile(csCsvFile).map(line => {
      CsEvent.fromCsv.fromFields(line.split(","))
    })

    val psEvents: RDD[PsEvent] = sqc.sparkContext.textFile(psCsvFile).map(line => {
      PsEvent.fromCsv.fromFields(line.split(","))
    })

    val events: RDD[Event] = eventReader(csEvents, psEvents).toEvent

    // for write to parquet in local
    sqc.createSchemaRDD(events).saveAsParquetFile("file:///tmp/parquet" + System.currentTimeMillis())
    //sqc.createSchemaRDD(events).saveAsParquetFile("hdfs://host:8020/user/root/parquet" + System.currentTimeMillis())

    // for write to avro + parquet in local
    AvroParquetRDDUtils.writeParquetRDD(sqc.sparkContext, events.map[(Void, Event)](event => (null, event)), Event.SCHEMA$, "file:///tmp/avro_parquet" + System.currentTimeMillis())
    //AvroParquetRDDUtils.writeParquetRDD(sqc.sparkContext, events.map[(Void, Event)](event => (null, event)), Event.SCHEMA$, "hdfs://host:8020/user/root/avro_parquet" + System.currentTimeMillis())

  }

}
