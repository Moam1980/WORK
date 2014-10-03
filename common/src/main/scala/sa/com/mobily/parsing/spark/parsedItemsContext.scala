/*
 * TODO: License goes here!
 */

package sa.com.mobily.parsing.spark

import scala.language.implicitConversions
import scala.reflect.ClassTag

import org.apache.spark.rdd.RDD

import sa.com.mobily.parsing.{ParsedItem, ParsingError}

class ParsedItemsUnwrapFunctions[T](self: RDD[ParsedItem[T]]) {

  def values[T:ClassTag]: RDD[T] = self.flatMap(_.value match {
    case Some(v) => Seq(v.asInstanceOf[T])
    case _ => Seq.empty
  })

  def errors: RDD[ParsingError] = self.flatMap(_.parsingError match {
    case Some(e) => Seq(e)
    case _ => Seq.empty
  })
}

trait ParsedItemsContext {

  implicit def parsedItemsUnwrapFunctions[T](wrappedItems: RDD[ParsedItem[T]]): ParsedItemsUnwrapFunctions[T] =
    new ParsedItemsUnwrapFunctions[T](wrappedItems)
}

object ParsedItemsContext extends ParsedItemsContext
