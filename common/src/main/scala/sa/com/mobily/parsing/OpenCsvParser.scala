/*
 * TODO: License goes here!
 */

package sa.com.mobily.parsing

import au.com.bytecode.opencsv.CSVParser
import sa.com.mobily.utils.EdmCoreUtils

/** Parser for CSV files.
  *
  * @constructor create a new parser.
  * @param separator: Delimiter to use for separating entries
  * @param quote: Character to use for quoted elements
  * @param escape: Character to use for escaping a separator or quote
  * @param strictQuotes: If true, characters outside the quotes are ignored
  * @param ignoreLeadingWhitespace: If true, white space in front of a quote in a field is ignored
  */
class OpenCsvParser(
    separator: Char = '|',
    quote: Char = OpenCsvParser.NonDefined,
    escape: Char = '\\',
    strictQuotes: Boolean = false,
    ignoreLeadingWhitespace: Boolean = true)
  extends CSVParser(separator, quote, escape, strictQuotes, ignoreLeadingWhitespace) with Serializable

object OpenCsvParser {

  final val NonDefined: Char = '\0'
}
