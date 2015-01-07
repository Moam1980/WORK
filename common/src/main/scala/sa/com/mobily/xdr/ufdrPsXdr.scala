/*
 * TODO: License goes here!
 */

package sa.com.mobily.xdr

import scala.language.{existentials, implicitConversions}

import org.apache.spark.sql._

import sa.com.mobily.event.{PsUfdrSource, Event}
import sa.com.mobily.parsing.{RowParser, CsvParser, OpenCsvParser}
import sa.com.mobily.user.User
import sa.com.mobily.utils.EdmCoreUtils

case class Duration(
    beginTime: Long,
    endTime: Long)

case class DurationMsel(
    beginTimeMsel: Int = 0,
    endTimeMsel: Int = 0)

case class Inet(
    ip: String,
    port: Int)

case class NetworkNode(
    sigIp: String,
    userIp: String)

case class Protocol(
    category: ProtocolCategory,
    id: Int)

case class UfdrPSXdrCell(
    rat: RadioAccessTechnology,
    lac: String,
    rac: String,
    sac: String,
    ci: String,
    tac: String,
    eci: String,
    mcc: String,
    mnc: String) {

  lazy val id: (Int, Int) =
    if (!lac.isEmpty)
      if (!sac.isEmpty) (EdmCoreUtils.hexToInt(lac), EdmCoreUtils.hexToInt(sac))
      else (EdmCoreUtils.hexToInt(lac), UfdrPSXdrCell.NonDefined)
    else if (!tac.isEmpty)
      if (!eci.isEmpty) (EdmCoreUtils.hexToInt(tac), EdmCoreUtils.hexToInt(eci))
      else if (!ci.isEmpty) (EdmCoreUtils.hexToInt(tac), EdmCoreUtils.hexToInt(ci))
      else (EdmCoreUtils.hexToInt(tac), UfdrPSXdrCell.NonDefined)
    else (UfdrPSXdrCell.NonDefined, UfdrPSXdrCell.NonDefined)


  def fields: Array[String] = Array(rat.identifier.toString, lac, rac, sac, ci, tac, eci, mcc, mnc)

  def idFields: Array[String] = Array(id._1.toString, id._2.toString)

  override def equals(other: Any): Boolean = other match {
    case that: UfdrPSXdrCell => (that canEqual this) && (this.id == that.id)
    case _ => false
  }

  override def hashCode: Int = id.hashCode

  def canEqual(other: Any): Boolean = other.isInstanceOf[UfdrPSXdrCell]
}

object UfdrPSXdrCell {

  final val NonDefined: Int = -1

  def header: Array[String] = Array("rat", "lac", "rac", "sac", "ci", "tac", "eci", "mcc", "mnc")

  def idHeader: Array[String] = Array("LacTac", "SacEci")
}

case class UfdrPsXdr(
    sid: Long,
    interfaceId: NetworkInterface,
    duration: Duration,
    protocol: Protocol,
    user: User,
    msInet: Inet,
    serverInet: Inet,
    apn: String,
    sgsnNetworkNode: NetworkNode,
    ggsnNetworkNode: NetworkNode,
    ranNeUserIp: String,
    cell: UfdrPSXdrCell,
    transferStats: TransferStats,
    host: Option[String],
    firstUri: Option[String],
    userAgent: Option[String],
    durationMsel : DurationMsel,
    clickToContent: Option[Long]) {

  def toEvent: Event = {
    Event(
      user = user,
      beginTime = duration.beginTime,
      endTime = duration.endTime,
      lacTac = cell.id._1,
      cellId = cell.id._2,
      source = PsUfdrSource,
      eventType = Some(protocol.category.identifier + "." + protocol.id),
      subsequentLacTac = None,
      subsequentCellId = None)
  }
}

case class UfdrPsXdrHierarchy(
    hourTime: String,
    cell: UfdrPSXdrCell,
    user: User,
    protocol: Protocol) {

  def fields: Array[String] =
    Array(hourTime) ++
      cell.idFields ++
      user.fields ++
      Array(protocol.category.identifier.toString, protocol.id.toString)
}

object UfdrPsXdrHierarchy {

  def header: Array[String] =
    Array("Date Hour") ++ UfdrPSXdrCell.idHeader ++ User.header ++ Array("category id", "protocol id")
}

case class UfdrPsXdrHierarchyAgg(
    hierarchy: UfdrPsXdrHierarchy,
    transferStats: TransferStats) {

  def fields: Array[String] = hierarchy.fields ++ transferStats.fields
}

object UfdrPsXdrHierarchyAgg {

  def header: Array[String] = UfdrPsXdrHierarchy.header ++ TransferStats.header
}


object UfdrPsXdr {

  final val lineCsvParserObject = new OpenCsvParser(separator = '|')

  implicit val fromCsv = new CsvParser[UfdrPsXdr] {

    override def lineCsvParser: OpenCsvParser = lineCsvParserObject

    override def fromFields(fields: Array[String]): UfdrPsXdr = {
      val fieldsTrim = fields.map(_.trim)
      val (firstChunk, restChunk) = fieldsTrim.splitAt(19) // scalastyle:ignore magic.number
      val (secondChunk, thirdChunk) = restChunk.splitAt(19) // scalastyle:ignore magic.number
      val Array(sidText, interfaceIdText, beginTimeText, endTimeText, protocolCategoryText, protocolIdText,
        msisdnText, imsiText, imeiText, msIpText, serverIpText, msPortText, serverPortText, apnText, sgsnSigIpText,
        ggsnSigIpText, sgsnUserIpText, ggsnUserIpText, ranNeUserIpText) = firstChunk
      val Array(ratText, lacText, racText, sacText, ciText, tacText, eciText, l4UlThroughputText, l4DwThroughputText,
        l4UlPacketsText, l4DwPacketsText, dataTransUlDurationText, dataTransDwDurationText, hostText, firstUriText,
        userAgentText, beginTimeMselText, endTimeMselText, ulLostRateText) = secondChunk
      val Array(dwLostRateText, clickToContentText, mccText, mncText) = thirdChunk

      UfdrPsXdr(
        sid = sidText.toLong,
        interfaceId = parseNetworkInterface(interfaceIdText),
        duration = Duration(beginTime = beginTimeText.toLong * 1000, endTime = endTimeText.toLong * 1000),
        protocol = Protocol(category = parseProtocolCategory(protocolCategoryText), id = protocolIdText.toInt),
        user = User(imei = imeiText, imsi = imsiText, msisdn = EdmCoreUtils.parseLong(msisdnText).getOrElse(0L)),
        msInet = Inet(ip = msIpText, port = msPortText.toInt),
        serverInet = Inet(ip = serverIpText, port = serverPortText.toInt),
        apn = apnText,
        sgsnNetworkNode = NetworkNode(sigIp = sgsnSigIpText, userIp = sgsnUserIpText),
        ggsnNetworkNode = NetworkNode(sigIp = ggsnSigIpText, userIp = ggsnUserIpText),
        ranNeUserIp = ranNeUserIpText,
        cell = UfdrPSXdrCell(
          rat = parseRadioAccessTechnology(ratText),
          lac = lacText,
          rac = racText,
          sac = sacText,
          ci = ciText,
          tac = tacText,
          eci = eciText,
          mcc = mccText,
          mnc = mncText),
        transferStats = TransferStats(
          l4UlThroughput = l4UlThroughputText.toLong,
          l4DwThroughput = l4DwThroughputText.toLong,
          l4UlPackets = l4UlPacketsText.toInt,
          l4DwPackets = l4DwPacketsText.toInt,
          dataTransUlDuration = EdmCoreUtils.parseLong(dataTransUlDurationText).getOrElse(0L),
          dataTransDwDuration = EdmCoreUtils.parseLong(dataTransDwDurationText).getOrElse(0L),
          ulLostRate = EdmCoreUtils.parseInt(ulLostRateText).getOrElse(0),
          dwLostRate = EdmCoreUtils.parseInt(dwLostRateText).getOrElse(0)),
        host = Some(hostText),
        firstUri = Some(firstUriText),
        userAgent = Some(userAgentText),
        durationMsel = DurationMsel(
          beginTimeMsel = EdmCoreUtils.parseInt(beginTimeMselText).getOrElse(0),
          endTimeMsel = EdmCoreUtils.parseInt(endTimeMselText).getOrElse(0)),
        clickToContent = EdmCoreUtils.parseLong(clickToContentText))
    }
  }

  implicit val fromRow = new RowParser[UfdrPsXdr] {

    override def fromRow(row: Row): UfdrPsXdr = { // scalastyle:ignore method.length
      val (firstChunk, restChunk) = row.toSeq.splitAt(11) // scalastyle:ignore magic.number
      val (secondChunk, thirdChunk) = restChunk.splitAt(4) // scalastyle:ignore magic.number
      val Seq(sid, Seq(interfaceId), Seq(beginTime, endTime), Seq(Seq(category), id), Seq(imei, imsi, msisdn),
        Seq(msIp, msPort), Seq(serverIp, serverPort), apn, Seq(sgsnSigIp, sgsnUserIp), Seq(ggsnSigIp, ggsnUserIp),
        ranNeUserIp) = firstChunk
      val Seq(Seq(Seq(rat), lac, rac, sac, ci, tac, eci, mcc, mnc),
        Seq(l4UlThroughput, l4DwThroughput, l4UlPackets, l4DwPackets, dataTransUlDuration, dataTransDwDuration,
          ulLostRate, dwLostRate),
        host, firstUri) = secondChunk
      val Seq(userAgent, Seq(beginTimeMsel, endTimeMsel), clickToContent) = thirdChunk

      UfdrPsXdr(
        sid = sid.asInstanceOf[Long],
        interfaceId = parseNetworkInterface(interfaceId.asInstanceOf[Int]),
        duration = Duration(beginTime = beginTime.asInstanceOf[Long], endTime = endTime.asInstanceOf[Long]),
        protocol = Protocol(category = parseProtocolCategory(category.asInstanceOf[Int]), id = id.asInstanceOf[Int]),
        user = User(imei = imei.asInstanceOf[String], imsi = imsi.asInstanceOf[String],
          msisdn = msisdn.asInstanceOf[Long]),
        msInet = Inet(ip = msIp.asInstanceOf[String], port = msPort.asInstanceOf[Int]),
        serverInet = Inet(ip = serverIp.asInstanceOf[String], port = serverPort.asInstanceOf[Int]),
        apn = apn.asInstanceOf[String],
        sgsnNetworkNode = NetworkNode(sigIp = sgsnSigIp.asInstanceOf[String], userIp = sgsnUserIp.asInstanceOf[String]),
        ggsnNetworkNode = NetworkNode(sigIp = ggsnSigIp.asInstanceOf[String], userIp = ggsnUserIp.asInstanceOf[String]),
        ranNeUserIp = ranNeUserIp.asInstanceOf[String],
        cell = UfdrPSXdrCell(
          rat = parseRadioAccessTechnology(rat.asInstanceOf[Int]),
          lac = lac.asInstanceOf[String],
          rac = rac.asInstanceOf[String],
          sac = sac.asInstanceOf[String],
          ci = ci.asInstanceOf[String],
          tac = tac.asInstanceOf[String],
          eci = eci.asInstanceOf[String],
          mcc = mcc.asInstanceOf[String],
          mnc = mnc.asInstanceOf[String]),
        transferStats = TransferStats(
          l4UlThroughput = l4UlThroughput.asInstanceOf[Long],
          l4DwThroughput = l4DwThroughput.asInstanceOf[Long],
          l4UlPackets = l4UlPackets.asInstanceOf[Int],
          l4DwPackets = l4DwPackets.asInstanceOf[Int],
          dataTransUlDuration = EdmCoreUtils.longOrZero(dataTransUlDuration),
          dataTransDwDuration = EdmCoreUtils.longOrZero(dataTransDwDuration),
          ulLostRate = EdmCoreUtils.intOrZero(ulLostRate),
          dwLostRate = EdmCoreUtils.intOrZero(dwLostRate)),
        host = EdmCoreUtils.stringOption(host),
        firstUri = EdmCoreUtils.stringOption(firstUri),
        userAgent = EdmCoreUtils.stringOption(userAgent),
        durationMsel = DurationMsel(
            beginTimeMsel = EdmCoreUtils.intOrZero(beginTimeMsel),
            endTimeMsel = EdmCoreUtils.intOrZero(endTimeMsel)),
        clickToContent = EdmCoreUtils.longOption(clickToContent))
    }
  }

  def parseNetworkInterface(interfaceInt: Int): NetworkInterface = interfaceInt match {
    case Gn.identifier => Gn
    case IuPs.identifier => IuPs
    case Gb.identifier => Gb
    case S11.identifier => S11
    case S5S8.identifier => S5S8
    case S1Mme.identifier => S1Mme
    case S6a.identifier => S6a
  }

  def parseNetworkInterface(interfaceText: String): NetworkInterface = parseNetworkInterface(interfaceText.toInt)

  def parseProtocolCategory(protocolCategoryInt: Int): ProtocolCategory = // scalastyle:ignore cyclomatic.complexity
    protocolCategoryInt match {
      case P2P.identifier => P2P
      case Im.identifier => Im
      case VoIp.identifier => VoIp
      case WebBrowsing.identifier => WebBrowsing
      case Game.identifier => Game
      case Streaming.identifier => Streaming
      case Email.identifier => Email
      case FileAccess.identifier => FileAccess
      case NetworkStorage.identifier => NetworkStorage
      case Stock.identifier => Stock
      case Tunnelling.identifier => Tunnelling
      case Miscellaneous.identifier => Miscellaneous
      case SocialNetworking.identifier => SocialNetworking
      case SoftwareUpdate.identifier => SoftwareUpdate
  }

  def parseProtocolCategory(protocolCategoryText: String): ProtocolCategory =
    parseProtocolCategory(protocolCategoryText.toInt)

  def parseRadioAccessTechnology(racInt: Int): RadioAccessTechnology = racInt match {
    case Null.identifier => Null
    case Utran.identifier => Utran
    case Geran.identifier => Geran
    case WLan.identifier => WLan
    case Gan.identifier => Gan
    case HspaEvolution.identifier => HspaEvolution
    case EuTran.identifier => EuTran
  }

  def parseRadioAccessTechnology(racText: String): RadioAccessTechnology = parseRadioAccessTechnology(racText.toInt)
}
