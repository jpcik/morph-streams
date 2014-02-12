package es.upm.fi.oeg.morph.stream.rewriting
import org.slf4j.LoggerFactory
import org.scalatest.Matchers
import org.scalatest.FlatSpec

class SQLParserTest  extends FlatSpec with Matchers  {
  private val logger= LoggerFactory.getLogger(this.getClass)

  "sql query" should "parse projection attributes" in{
    val p=SQLParser.projections("SELECT s,p,o FROM datacell.srbin WHERE o=RainfallObservation")
    p.foreach(println)
    p.size shouldBe 3
  }
  "sql query" should "parse selection operators" in{
    val s=SQLParser.selections("SELECT s,p,o FROM datacell.srbin WHERE o=RainfallObservation and p=type and t=rat")
    s.get.foreach(println)
    s.get.size shouldBe 3
  }
  "sql query without selections" should "parse no selections" in{
    val s=SQLParser.selections("SELECT s,p,o FROM datacell.srbin")
    println(s)
    s.size shouldBe 0
  }
}
