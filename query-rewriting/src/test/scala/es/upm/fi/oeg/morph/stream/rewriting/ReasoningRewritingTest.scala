package es.upm.fi.oeg.morph.stream.rewriting

import org.slf4j.LoggerFactory
import com.hp.hpl.jena.query.QueryFactory
import collection.JavaConversions._
import es.upm.fi.oeg.siq.tools.ParameterUtils
import java.net.URI
import org.scalatest.Matchers
import org.scalatest.FlatSpec

class ReasoningRewritingTest extends FlatSpec with Matchers {
  private val logger= LoggerFactory.getLogger(this.getClass)
  
  private def srbench(q:String)=ParameterUtils.loadQuery("queries/srbench/"+q)
  private def ssn(q:String)=ParameterUtils.loadQuery("queries/ssn/"+q)
  private val srbenchR2rml=new URI("mappings/srbenchkyrie.ttl")
  
  private def rewrite(sparqlstr:String)={    
    val trans = new QueryRewriting(srbenchR2rml.toString,"kyrietest")
    trans.translate(sparqlstr)//.asInstanceOf[EsperQuery]
  }
    
  "Datalog query" should "be rewritten" in {
    logger.info("")
    val k=new Kyrie("src/test/resources/ontologies/sensordemo.owl")
    val q=QueryFactory.create("PREFIX rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#> " +
                              "PREFIX casas:<http://oeg-upm.net/onto/casas/> "+
    		"SELECT ?s WHERE { ?c a casas:Edificio }")
    //logger.debug(k.clausify(q).mkString(".."))
    //val q2=k.rewriteDatalogString("Q(?0) <- quantityKind(?0,?1), QuantityKind(?1)")
    		val q2=k.rewriteDatalogString("Q(?0,?1) <- observedBy(?0,?1)")
    //val q2=k.rewriteDatalogString("Q(?0) <- featureOfInterest(?0,?1),FeatureOfInterest(?1)")
    //val q2=k.rewriteDatalogString("Q(?0) <- observes(?0,?1) ,Property(?1)")
    logger.debug(q2.mkString("--"))   
  }
  
  "q1" should "be rewritten" in{
     val q=rewrite(ssn("q1.sparql"))
  }
  "q2" should "be rewritten" in{
     val q=rewrite(ssn("q2.sparql"))
  }
  "q3" should "be rewritten" in{
     val q=rewrite(ssn("q3.sparql"))
  }
  "q4" should "be rewritten" in{
     val q=rewrite(ssn("q4.sparql"))
  }
  "q5" should "be rewritten" in{
     val q=rewrite(ssn("q5.sparql"))
  }
  "q6" should "be rewritten" in{
     val q=rewrite(ssn("q6.sparql"))
  }
  "q7" should "be rewritten" in{
     val q=rewrite(ssn("q7.sparql"))
  }
  "q8" should "be rewritten" in{
     val q=rewrite(ssn("q8.sparql"))
  }
}
