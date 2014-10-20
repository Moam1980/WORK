/*
 * TODO: License goes here!
 */

package sa.com.mobily.event

import org.apache.avro.{AvroRuntimeException, Schema}

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
  var tac: String) extends org.apache.avro.specific.SpecificRecordBase with org.apache.avro.specific.SpecificRecord {

  override def getSchema: Schema = Event.SCHEMA$

  override def get(p1: Int): AnyRef = {
    p1 match {
      case 0 =>
        return beginTime
      case 1 =>
        return ci
      case 2 =>
        return eci
      case 3 =>
        return endTime
      case 4 =>
        return eventType
      case 5 =>
        return imei
      case 6 =>
        return imsi
      case 7 =>
        return ixc
      case 8 =>
        return lac
      case 9 =>
        return mcc
      case 10 =>
        return mnc
      case 11 =>
        return msisdn
      case 12 =>
        return rac
      case 13 =>
        return rat
      case 14 =>
        return sac
      case 15 =>
        return tac
      case _ =>
        throw new AvroRuntimeException("Bad index")
    }
  }

  override def put(p1: Int, p2: scala.Any): Unit = {

    p1 match {
      case 0 =>
        beginTime = p2.asInstanceOf[java.lang.Long]
      case 1 =>
        ci = p2.asInstanceOf[String]
      case 2 =>
        eci = p2.asInstanceOf[String]
      case 3 =>
        endTime = p2.asInstanceOf[java.lang.Long]
      case 4 =>
        eventType = p2.asInstanceOf[java.lang.Short]
      case 5 =>
        imei = p2.asInstanceOf[java.lang.Long]
      case 6 =>
        imsi = p2.asInstanceOf[java.lang.Long]
      case 7 =>
        ixc = p2.asInstanceOf[String]
      case 8 =>
        lac = p2.asInstanceOf[String]
      case 9 =>
        mcc = p2.asInstanceOf[String]
      case 10 =>
        mnc = p2.asInstanceOf[String]
      case 11 =>
        msisdn = p2.asInstanceOf[java.lang.Long]
      case 12 =>
        rac = p2.asInstanceOf[String]
      case 13 =>
        rat = p2.asInstanceOf[java.lang.Short]
      case 14 =>
        sac = p2.asInstanceOf[String]
      case 15 =>
        tac = p2.asInstanceOf[String]
      case _ =>
        throw new AvroRuntimeException("Bad index")
    }

  }
}

object Event {

  final val SCHEMA$: Schema = new Schema.Parser().parse("{\"type\":\"record\",\"name\":\"Event\",\"namespace\":\"sa.com.mobily.event\",\"fields\":[ { \"name\":\"beginTime\", \"type\":\"long\", \"default\":\"long\" }, { \"name\":\"ci\", \"type\":\"string\", \"default\":\"unknown\" }, { \"name\":\"eci\", \"type\":\"string\", \"default\":\"unknown\" }, { \"name\":\"endTime\", \"type\":\"long\", \"default\":\"long\" }, { \"name\":\"eventType\", \"type\":\"int\", \"default\":\"int\" }, { \"name\":\"imei\", \"type\":\"long\", \"default\":\"long\" }, { \"name\":\"imsi\", \"type\":\"long\", \"default\":\"long\" }, { \"name\":\"ixc\", \"type\":\"string\", \"default\":\"unknown\" }, { \"name\":\"lac\", \"type\":\"string\", \"default\":\"unknown\" }, { \"name\":\"mcc\", \"type\":\"string\", \"default\":\"unknown\" }, { \"name\":\"mnc\", \"type\":\"string\", \"default\":\"unknown\" }, { \"name\":\"msisdn\", \"type\":\"long\", \"default\":\"long\" }, { \"name\":\"rac\", \"type\":\"string\", \"default\":\"unknown\" }, { \"name\":\"rat\", \"type\":\"int\", \"default\":\"int\" }, { \"name\":\"sac\", \"type\":\"string\", \"default\":\"unknown\" }, { \"name\":\"tac\", \"type\":\"string\", \"default\":\"unknown\" } ]}")

}

