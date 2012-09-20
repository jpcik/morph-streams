package es.upm.fi.oeg.morph.stream.rewriting
import org.scalatest.junit.JUnitSuite
import org.scalatest.junit.ShouldMatchersForJUnit
import org.scalatest.prop.Checkers
import com.weiglewilczek.slf4s.Logging
import org.apache.log4j.PropertyConfigurator
import es.upm.fi.oeg.morph.common.ParameterUtils._
import org.junit.Before
import org.junit.Test
import es.upm.fi.oeg.morph.common.ParameterUtils
import java.net.URI

class SRBenchRewritingTest extends JUnitSuite with ShouldMatchersForJUnit with Checkers with Logging {
  PropertyConfigurator.configure(classOf[SRBenchRewritingTest].getClassLoader().getResource("config/log4j.properties"));
  val props = load(getClass.getClassLoader.getResourceAsStream("config/config_memoryStore.properties"));
  val mappingUri=new URI("mappings/srbench.ttl")
  val trans = new QueryRewriting(props,mappingUri.toString);    
  
  @Before def initialize() {}
 
  @Test def testJoinPatternMatching() {     
    val query = loadQuery("queries/srbench/join-pattern-matching.sparql")
    logger.info(query)    
    trans.translate(query)
  }
  @Test def testBasicPatternMatching() {     
    val query = loadQuery("queries/srbench/basic-pattern-matching.sparql")
    logger.info(query)    
    trans.translate(query)
  }
  
  @Test def testOptionalPatternMatching() {     
    val query = loadQuery("queries/srbench/optional-pattern-matching.sparql")
    logger.info(query)    
    trans.translate(query)
  }

  @Test def testVariablePredicate() {     
    val query = loadQuery("queries/srbench/variable-predicate.sparql")
    logger.info(query)    
    trans.translate(query)
  }

  @Test def testMaxAggregate() {     
    val query = loadQuery("queries/srbench/max-aggregate.sparql")
    logger.info(query)    
    trans.translate(query)
  }

  
}