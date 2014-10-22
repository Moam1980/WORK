/*
 * TODO: License goes here!
 */

package sa.com.mobily.parsing.spark

import scala.language.implicitConversions
import scala.reflect.ClassTag

import org.apache.spark.rdd.RDD

import sa.com.mobily.parsing.{ParsedItem, ParsingError}

class ParsedItemsUnwrapFunctions[T:ClassTag](self: RDD[ParsedItem[T]]) {

  def values: RDD[T] = self.flatMap(_.value match {
    case Some(v) => Seq(v)
    case _ => Seq.empty
  })

  def errors: RDD[ParsingError] = self.flatMap(_.parsingError match {
    case Some(e) => Seq(e)
    case _ => Seq.empty
  })
}

trait ParsedItemsDsl {

  implicit def parsedItemsUnwrapFunctions[T:ClassTag](
      wrappedItems: RDD[ParsedItem[T]]): ParsedItemsUnwrapFunctions[T] =
    new ParsedItemsUnwrapFunctions[T](wrappedItems)
}

object ParsedItemsDsl extends ParsedItemsDsl
