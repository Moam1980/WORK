/*
 * TODO: License goes here!
 */

package sa.com.mobily.xdr

/** ProtocolCategory */
sealed case class ProtocolCategory(val identifier: Int)

object P2P extends ProtocolCategory(identifier = 1) // scalastyle:ignore magic.number
object Im extends ProtocolCategory(identifier = 2) // scalastyle:ignore magic.number
object VoIp extends ProtocolCategory(identifier = 3) // scalastyle:ignore magic.number
object WebBrowsing extends ProtocolCategory(identifier = 4) // scalastyle:ignore magic.number
object Game extends ProtocolCategory(identifier = 5) // scalastyle:ignore magic.number
object Streaming extends ProtocolCategory(identifier = 6) // scalastyle:ignore magic.number
object Email extends ProtocolCategory(identifier = 9) // scalastyle:ignore magic.number
object FileAccess extends ProtocolCategory(identifier = 10) // scalastyle:ignore magic.number
object NetworkStorage extends ProtocolCategory(identifier = 12) // scalastyle:ignore magic.number
object Stock extends ProtocolCategory(identifier = 15) // scalastyle:ignore magic.number
object Tunnelling extends ProtocolCategory(identifier = 16) // scalastyle:ignore magic.number
object Miscellaneous extends ProtocolCategory(identifier = 17) // scalastyle:ignore magic.number
object SocialNetworking extends ProtocolCategory(identifier = 18) // scalastyle:ignore magic.number
object SoftwareUpdate extends ProtocolCategory(identifier = 19) // scalastyle:ignore magic.number
