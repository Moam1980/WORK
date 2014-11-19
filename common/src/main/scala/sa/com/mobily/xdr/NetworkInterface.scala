/*
 * TODO: License goes here!
 */

package sa.com.mobily.xdr

/** Interface */
sealed case class NetworkInterface(identifier: Int)

object Gn extends NetworkInterface(identifier = 1) // scalastyle:ignore magic.number
object IuPs extends NetworkInterface(identifier = 3) // scalastyle:ignore magic.number
object Gb extends NetworkInterface(identifier = 4) // scalastyle:ignore magic.number
object S11 extends NetworkInterface(identifier = 5) // scalastyle:ignore magic.number
object S5S8 extends NetworkInterface(identifier = 6) // scalastyle:ignore magic.number
object S1Mme extends NetworkInterface(identifier = 7) // scalastyle:ignore magic.number
object S6a extends NetworkInterface(identifier = 8) // scalastyle:ignore magic.number
