package es.upm.fi.oeg.morph.stream.rewriting
import org.scalatest.prop.Checkers
import org.scalatest.junit.ShouldMatchersForJUnit
import org.scalatest.junit.JUnitSuite
import org.junit.Test
import org.slf4j.LoggerFactory

class SQLParserTest  extends JUnitSuite with ShouldMatchersForJUnit with Checkers {
  private val logger= LoggerFactory.getLogger(this.getClass)

  @Test def parseProjections{
    val p=SQLParser.projections("SELECT s,p,o FROM datacell.srbin WHERE o=RainfallObservation")
    p.foreach(println)
  }
  @Test def parseSelections{
    val p=SQLParser.selections("SELECT s,p,o FROM datacell.srbin WHERE o=RainfallObservation and p=type and t=rat")
    p.get.foreach(println)
  }
  @Test def parseNoSelections{
    val p=SQLParser.selections("SELECT s,p,o FROM datacell.srbin")
    println(p)
  }
}
