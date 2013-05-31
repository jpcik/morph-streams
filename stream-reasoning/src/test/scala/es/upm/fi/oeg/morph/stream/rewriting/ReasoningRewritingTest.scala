package es.upm.fi.oeg.morph.stream.rewriting

import org.slf4j.LoggerFactory
import org.scalatest.prop.Checkers
import org.scalatest.junit.ShouldMatchersForJUnit
import org.scalatest.junit.JUnitSuite
import org.junit.Before
import org.junit.Test
import scala.io.Source
import com.hp.hpl.jena.query.QueryFactory
import collection.JavaConversions._
import com.hp.hpl.jena.query.Query
import es.upm.fi.dia.oeg.newrqr.DatalogSPARQLConversor
import org.oxford.comlab.requiem.parser.ELHIOParser
import org.oxford.comlab.requiem.rewriter.TermFactory
import org.oxford.comlab.requiem.rewriter.PreprocessRewriter
import java.util.ArrayList
import es.upm.fi.dia.oeg.newrqr.ISI2RQRLexer
import es.upm.fi.dia.oeg.newrqr.ISI2RQRParser
import org.antlr.runtime.ANTLRStringStream
import org.oxford.comlab.requiem.rewriter.Clause
import org.antlr.runtime.CommonTokenStream
import es.upm.fi.oeg.morph.kyrie.Kyrie
import es.upm.fi.oeg.siq.tools.ParameterUtils
import java.net.URI
import es.upm.fi.oeg.sparqlstream.SparqlStream
import es.upm.fi.oeg.morph.stream.esper.EsperQuery

class ReasoningRewritingTest extends JUnitSuite with ShouldMatchersForJUnit with Checkers {
  private val logger= LoggerFactory.getLogger(this.getClass)
  val onto=Source.fromInputStream(getClass.getResourceAsStream("/casas.owl")).getLines.mkString
  val props = ParameterUtils.load(getClass.getClassLoader.getResourceAsStream("config/siq.properties"))
  
  private def srbench(q:String)=ParameterUtils.loadQuery("queries/srbench/"+q)
  private def ssn(q:String)=ParameterUtils.loadQuery("queries/ssn/"+q)
  private val srbenchR2rml=new URI("mappings/srbench.ttl")
  
  private def rewrite(sparqlstr:String)={    
    val trans = new ExpansionQueryRewriting(props,srbenchR2rml.toString)
    trans.translate(SparqlStream.parse(sparqlstr)).asInstanceOf[EsperQuery]
  }
  
  @Before def initialize() {}

  @Test def testTranslateDatalog() {
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
  @Test def testQ1(){
     val q=rewrite(ssn("q1.sparql"))
  }
  @Test def testQ2(){
     val q=rewrite(ssn("q2.sparql"))
  }
  @Test def testQ3(){
     val q=rewrite(ssn("q3.sparql"))
  }
  @Test def testQ4(){
     val q=rewrite(ssn("q4.sparql"))
  }
  @Test def testQ5(){
     val q=rewrite(ssn("q5.sparql"))
  }
  @Test def testQ6(){
     val q=rewrite(ssn("q6.sparql"))
  }
  @Test def testQ7(){
     val q=rewrite(ssn("q7.sparql"))
  }
}
