package es.upm.fi.oeg.morph.stream.rewriting

import org.scalatest.junit.ShouldMatchersForJUnit
import org.scalatest.prop.Checkers
import org.scalatest.junit.JUnitSuite
import org.slf4j.LoggerFactory
import es.upm.fi.oeg.siq.tools.ParameterUtils
import java.net.URI
import org.junit.Before
import org.junit.Test

class NoReasoningTest extends JUnitSuite with ShouldMatchersForJUnit with Checkers {
  private val logger= LoggerFactory.getLogger(this.getClass)
  //val onto=Source.fromInputStream(getClass.getResourceAsStream("/casas.owl")).getLines.mkString
  //val props = ParameterUtils.load(getClass.getClassLoader.getResourceAsStream("config/siq.properties"))
  
  private def srbench(q:String)=ParameterUtils.loadQuery("queries/srbench/"+q)
  private def ssn(q:String)=ParameterUtils.loadQuery("queries/ssn/"+q)
  private val srbenchR2rml=new URI("mappings/srbenchkyrie.ttl")
  
  private def rewrite(sparqlstr:String)={    
    val trans = new QueryRewriting(srbenchR2rml.toString,"noreasoning")
    trans.translate(sparqlstr)//.asInstanceOf[EsperQuery]
  }
  
  @Before def initialize() {}

  @Test def testQ8(){
     val q=rewrite(ssn("q8.sparql"))
  }

  @Test def testQ9(){
     val q=rewrite(ssn("q9.sparql"))
  }
  
}