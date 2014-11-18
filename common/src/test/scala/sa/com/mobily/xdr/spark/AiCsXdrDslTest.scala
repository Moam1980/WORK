/*
 * TODO: License goes here!
 */

package sa.com.mobily.xdr.spark

import scala.reflect.io.File

import org.scalatest.{FlatSpec, ShouldMatchers}

import sa.com.mobily.utils.{LocalSparkSqlContext}

class AiCsXdrDslTest extends FlatSpec with ShouldMatchers with LocalSparkSqlContext {

  import AiCsXdrDsl._

  trait WithAiCsEvents {
    val aiCsXdrLine1 = "0,0149b9829192,0149b982d8e6,000392,000146,0,201097345297,01,_,_,1751,1,8,21,_," +
        "420034104770740,61c5f3e5,0839,517d,_,_,_,00004330,_,_,00004754,00000260,00000702,_,_,0,_,9,_,_,_," +
        "10,_,_,0839,517d,_,_,_,_,0839,_,_,10,_,_,03,61c5f3e5,3523870633105423,00,_,0,_,_,0,0,254,_,_,_,_,1,_,0" +
        ",_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,d8000c00,80000,230e0000,0,0,16,_,_,_,_,00,0839,_,_,_,_,424e,_,0," +
        "ba,11,_,1,0,60"
    val aiCsXdrLine2 = "0149b982da5b,000251,000146,_,_,_,_,_,_,_,1"
    val aiCsXdrLine3 = "4,0149b982d580,0149b982da5b,000251,000146,_,_,_,_,_,_,_,1,_,_,420032370264779,b0c5b874," +
        "0a3e,e882,_,_,_,_,_,_,000004db,_,_,_,_,0,_,9,_,_,_,_,_,_,0a3e,e882,_,_,420,03,0887,_,_,_,_,_,_," +
        "3547240635999912,b0c5b874,00,0,_,_,_,0,0,254,_,_,_,_,_,_,0,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_," +
        "18000c00,56000000,0,0,0,_,_,_,_,_,00,0a3e,_,_,_,_,_,3,0,_,_,_,1,_,60"
    val aiCsXdrs = sc.parallelize(List(aiCsXdrLine1, aiCsXdrLine2, aiCsXdrLine3))
  }

  "AiCsXdrDsl" should "get correctly parsed AiCS events" in new WithAiCsEvents {
    aiCsXdrs.toAiCsXdr.count should be(2)
  }

  it should "get errors when parsing AiCsXdrs" in new WithAiCsEvents {
    aiCsXdrs.toAiCsXdrErrors.count should be(1)
  }

  it should "get both correctly and wrongly parsed AiCsXdrs" in new WithAiCsEvents {
    aiCsXdrs.toParsedAiCsXdr.count should be(3)
  }

  it should "save events in parquet" in new WithAiCsEvents {
    val path = File.makeTemp().name
    val events = aiCsXdrs.toAiCsXdr
    events.saveAsParquetFile(path)
    sqc.parquetFile(path).toAiCsXdr.collect.sameElements(events.collect) should be(true)
    File(path).deleteRecursively
  }
}
