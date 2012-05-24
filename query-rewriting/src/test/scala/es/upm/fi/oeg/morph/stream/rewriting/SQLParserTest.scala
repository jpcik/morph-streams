package es.upm.fi.oeg.morph.stream.rewriting
import org.scalatest.prop.Checkers
import org.scalatest.junit.ShouldMatchersForJUnit
import org.scalatest.junit.JUnitSuite
import com.weiglewilczek.slf4s.Logging
import org.junit.Test

class SQLParserTest  extends JUnitSuite with ShouldMatchersForJUnit with Checkers with Logging {
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
