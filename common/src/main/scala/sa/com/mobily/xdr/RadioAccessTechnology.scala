/*
 * TODO: License goes here!
 */

package sa.com.mobily.xdr

/** RadioAccessTechnology */
sealed case class RadioAccessTechnology(identifier: Int)

object Null extends RadioAccessTechnology(identifier = 0) // scalastyle:ignore magic.number
object Utran extends RadioAccessTechnology(identifier = 1) // scalastyle:ignore magic.number
object Geran extends RadioAccessTechnology(identifier = 2) // scalastyle:ignore magic.number
object WLan extends RadioAccessTechnology(identifier = 3) // scalastyle:ignore magic.number
object Gan extends RadioAccessTechnology(identifier = 4) // scalastyle:ignore magic.number
object HspaEvolution extends RadioAccessTechnology(identifier = 5) // scalastyle:ignore magic.number
object EuTran extends RadioAccessTechnology(identifier = 6) // scalastyle:ignore magic.number
