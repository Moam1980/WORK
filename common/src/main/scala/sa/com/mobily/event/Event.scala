/*
 * TODO: License goes here!
 */

package sa.com.mobily.event

import org.apache.avro.Schema

/**
  */
case class Event(
  var beginTime: java.lang.Long,
  var ci: String,
  var eci: String,
  var endTime: java.lang.Long,
  var eventType: java.lang.Short,
  var imei: java.lang.Long,
  var imsi: java.lang.Long,
  var ixc: String,
  var lac: String,
  var mcc: String,
  var mnc: String,
  var msisdn: java.lang.Long,
  var rac: String,
  var rat: java.lang.Short,
  var sac: String,
  var tac: String) {

}

object Event {

  final val SCHEMA$: Schema = new Schema.Parser().parse("{\"type\":\"record\",\"name\":\"Event\",\"namespace\":\"sa.com.mobily.event\",\"fields\":[ { \"name\":\"beginTime\", \"type\":\"long\", \"default\":\"long\" }, { \"name\":\"ci\", \"type\":\"string\", \"default\":\"unknown\" }, { \"name\":\"eci\", \"type\":\"string\", \"default\":\"unknown\" }, { \"name\":\"endTime\", \"type\":\"long\", \"default\":\"long\" }, { \"name\":\"eventType\", \"type\":\"int\", \"default\":\"int\" }, { \"name\":\"imei\", \"type\":\"long\", \"default\":\"long\" }, { \"name\":\"imsi\", \"type\":\"long\", \"default\":\"long\" }, { \"name\":\"ixc\", \"type\":\"string\", \"default\":\"unknown\" }, { \"name\":\"lac\", \"type\":\"string\", \"default\":\"unknown\" }, { \"name\":\"mcc\", \"type\":\"string\", \"default\":\"unknown\" }, { \"name\":\"mnc\", \"type\":\"string\", \"default\":\"unknown\" }, { \"name\":\"msisdn\", \"type\":\"long\", \"default\":\"long\" }, { \"name\":\"rac\", \"type\":\"string\", \"default\":\"unknown\" }, { \"name\":\"rat\", \"type\":\"int\", \"default\":\"int\" }, { \"name\":\"sac\", \"type\":\"string\", \"default\":\"unknown\" }, { \"name\":\"tac\", \"type\":\"string\", \"default\":\"unknown\" } ]}")

}

