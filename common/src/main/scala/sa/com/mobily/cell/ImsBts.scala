/*
 * TODO: License goes here!
 */

package sa.com.mobily.cell

case class ImsBts(
    vendor: String,
    technology: Technology,
    node: String,
    parentBsc: String,
    longitude: String,
    latitude: String,
    ipAddress: String,
    netmask: String) {

  lazy val id: String = ImsBts.parseIdFromNode(node)

  def fields: Array[String] =
    Array(vendor, technology.identifier, id, node, parentBsc, longitude, latitude, ipAddress, netmask)
}

object ImsBts {

  private val CompositeNode = "_G12-TG-"

  def header: Array[String] =
    Array("vendor", "technology", "id", "node", "parentBsc", "longitude", "latitude", "ipAddress", "netmask")

  def parseIdFromNode(nodeText: String): String =
    if (nodeText.trim.contains(CompositeNode)) {
      val id = nodeText.substring(0, nodeText.trim.indexOf(CompositeNode)).split("_").last
      if (id.startsWith("G")) id.substring(1)
      else id
    } else nodeText

  def merge(imsBts1: ImsBts, imsBts2: ImsBts): ImsBts = {
    require(imsBts1.vendor == imsBts2.vendor && imsBts1.technology == imsBts2.technology &&
      imsBts1.node == imsBts2.node)
    imsBts1.copy(
      longitude = if (imsBts1.longitude.isEmpty) imsBts2.longitude else imsBts1.longitude,
      latitude = if (imsBts1.latitude.isEmpty) imsBts2.latitude else imsBts1.latitude,
      ipAddress = if (imsBts1.ipAddress.isEmpty) imsBts2.ipAddress else imsBts1.ipAddress,
      netmask = if (imsBts1.netmask.isEmpty) imsBts2.netmask else imsBts1.netmask)
  }
}
