/*
 * TODO: License goes here!
 */

package sa.com.mobily.event

import java.lang.{Long, Short}
import scala.util.{Failure, Success, Try}

import org.apache.avro.Schema

import sa.com.mobily.parsing.CsvParser

/**
  */
case class PsEvent(
  var beginTime: Long,
  var ci: String,
  var eci: String,
  var endTime: Long,
  var eventType: Short,
  var imei: Long,
  var imsi: Long,
  var ixc: Option[String],
  var lac: String,
  var mcc: Option[String],
  var mnc: Option[String],
  var msisdn: Long,
  var rac: String,
  var rat: Short,
  var sac: String,
  var tac: String) extends org.apache.avro.specific.SpecificRecordBase with org.apache.avro.specific.SpecificRecord {

  override def getSchema: Schema = PsEvent.SCHEMA$

  override def get(p1: Int): AnyRef = {
    p1 match {
      case 0 => beginTime
      case 1 => ci
      case 2 => eci
      case 3 => endTime
      case 4 => eventType
      case 5 => imei
      case 6 => imsi
      case 7 => ixc
      case 8 => lac
      case 9 => mcc
      case 10 => mnc
      case 11 => msisdn
      case 12 => rac
      case 13 => rat
      case 14 => sac
      case 15 => tac
    }
  }

  override def put(p1: Int, p2: Any): Unit = {

    p1 match {
      case 0 => beginTime = p2.asInstanceOf[Long]
      case 1 => ci = p2.asInstanceOf[String]
      case 2 => eci = p2.asInstanceOf[String]
      case 3 => endTime = p2.asInstanceOf[Long]
      case 4 => eventType = p2.asInstanceOf[Short]
      case 5 => imei = p2.asInstanceOf[Long]
      case 6 => imsi = p2.asInstanceOf[Long]
      case 7 => ixc = Try(p2.asInstanceOf[String]) match {
        case Success(v) => Some(v)
        case Failure(e) => None
      }
      case 8 => lac = p2.asInstanceOf[String]
      case 9 => mcc = Try(p2.asInstanceOf[String]) match {
        case Success(v) => Some(v)
        case Failure(e) => None
      }
      case 10 => mnc = Try(p2.asInstanceOf[String]) match {
        case Success(v) => Some(v)
        case Failure(e) => None
      }
      case 11 => msisdn = p2.asInstanceOf[Long]
      case 12 => rac = p2.asInstanceOf[String]
      case 13 => rat = p2.asInstanceOf[Short]
      case 14 => sac = p2.asInstanceOf[String]
      case 15 => tac = p2.asInstanceOf[String]
    }
  }
}

object PsEvent {

  final val SCHEMA$: Schema = new Schema.Parser().parse("{\"type\":\"record\",\"name\":\"CsEvent\"," +
    "\"namespace\":\"sa.com.mobily.event\",\"fields\":[ { \"name\":\"beginTime\", \"type\":\"long\", " +
    "\"default\":\"long\" }, { \"name\":\"ci\", \"type\":\"string\", \"default\":\"unknown\" }, { \"name\":\"eci\", " +
    "\"type\":\"string\", \"default\":\"unknown\" }, { \"name\":\"endTime\", \"type\":\"long\", " +
    "\"default\":\"long\" }, { \"name\":\"eventType\", \"type\":\"int\", \"default\":\"int\" }, { \"name\":\"imei\"," +
    " \"type\":\"long\", \"default\":\"long\" }, { \"name\":\"imsi\", \"type\":\"long\", \"default\":\"long\" }, " +
    "{ \"name\":\"ixc\", \"type\":\"string\", \"default\":\"unknown\" }, { \"name\":\"lac\", \"type\":\"string\", " +
    "\"default\":\"unknown\" }, { \"name\":\"mcc\", \"type\":\"string\", \"default\":\"unknown\" }, " +
    "{ \"name\":\"mnc\", \"type\":\"string\", \"default\":\"unknown\" }, { \"name\":\"msisdn\", \"type\":\"long\", " +
    "\"default\":\"long\" }, { \"name\":\"rac\", \"type\":\"string\", \"default\":\"unknown\" }, { \"name\":\"rat\"," +
    " \"type\":\"int\", \"default\":\"int\" }, { \"name\":\"sac\", \"type\":\"string\", \"default\":\"unknown\" }, " +
    "{ \"name\":\"tac\", \"type\":\"string\", \"default\":\"unknown\" } ]}")

  implicit val fromCsv = new CsvParser[PsEvent] {

    override val delimiter: String = ","

    override def fromFields(fields: Array[String]): PsEvent = {

      val beginTime: Long = fields(4).toLong
      val ci: String = fields(22)
      val eci: String = fields(24)
      val endTime: Long = fields(5).toLong
      val eventType: Short = fields(7).toShort
      val imei: Long = fields(3).toLong
      val imsi: Long = fields(1).toLong
      val ixc: Option[String] = Try(null) match {
        case Success(v) => Some(v)
        case Failure(e) => None
      }
      val lac: String = fields(19)
      val mcc: Option[String] = Try(null) match {
        case Success(v) => Some(v)
        case Failure(e) => None
      }
      val mnc: Option[String] = Try(null) match {
        case Success(v) => Some(v)
        case Failure(e) => None
      }
      val msisdn: Long = fields(0).toLong
      val rac: String = fields(20)
      val rat: Short = fields(18).toShort
      val sac: String = fields(21)
      val tac: String = fields(23)

      PsEvent(beginTime, ci, eci, endTime, eventType, imei, imsi, ixc, lac, mcc, mnc, msisdn, rac, rat, sac, tac)

    }
  }
}

