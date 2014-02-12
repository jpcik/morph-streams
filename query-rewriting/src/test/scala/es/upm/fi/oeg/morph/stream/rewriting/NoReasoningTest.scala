package es.upm.fi.oeg.morph.stream.rewriting

import org.slf4j.LoggerFactory
import es.upm.fi.oeg.siq.tools.ParameterUtils
import java.net.URI
import org.scalatest.FlatSpec
import org.scalatest.Matchers

class NoReasoningTest  extends FlatSpec with Matchers  {
  private val logger= LoggerFactory.getLogger(this.getClass)
  
  private def srbench(q:String)=ParameterUtils.loadQuery("queries/srbench/"+q)
  private def ssn(q:String)=ParameterUtils.loadQuery("queries/ssn/"+q)
  private val srbenchR2rml=new URI("mappings/srbenchkyrie.ttl")
  
  private def rewrite(sparqlstr:String)={    
    val trans = new QueryRewriting(srbenchR2rml.toString,"noreasoning")
    trans.translate(sparqlstr)//.asInstanceOf[EsperQuery]
  }
  
  "q8" should "be rewritten" in{
     val q=rewrite(ssn("q8.sparql"))
  }

  "q9" should "be rewritten" in{
     val q=rewrite(ssn("q9.sparql"))
  }
  
}