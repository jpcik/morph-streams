package es.upm.fi.oeg.morph.stream.rewriting
import es.upm.fi.oeg.morph.common.ParameterUtils._
import es.upm.fi.oeg.morph.common.ParameterUtils
import java.net.URI
import org.slf4j.LoggerFactory
import org.scalatest.FlatSpec
import org.scalatest.Matchers

class SRBenchRewritingTest extends FlatSpec with Matchers {
  private val logger= LoggerFactory.getLogger(this.getClass)

  val mappingUri=new URI("mappings/srbench.ttl")
  val trans = new QueryRewriting(mappingUri.toString,"default")    
  
  "join pattern matching query" should "be rewritten" in {     
    val query = loadQuery("queries/srbench/join-pattern-matching.sparql")
    logger.info(query)    
    trans.translate(query)
  }
  "basic pattern matching query" should "be rewritten" in {     
    val query = loadQuery("queries/srbench/basic-pattern-matching.sparql")
    logger.info(query)    
    trans.translate(query)
  }
  
  "optional pattern matching query" should "be rewritten" in {     
    val query = loadQuery("queries/srbench/optional-pattern-matching.sparql")
    logger.info(query)    
    trans.translate(query)
  }

  "variable predicate query" should "be rewritten" in {     
    val query = loadQuery("queries/srbench/variable-predicate.sparql")
    logger.info(query)    
    trans.translate(query)
  }

  "aggregate query" should "be rewritten" ignore {     
    val query = loadQuery("queries/srbench/max-aggregate.sparql")
    logger.info(query)    
    trans.translate(query)
  }
  "filter value query" should "be rewritten" in {     
    val query = loadQuery("queries/srbench/filter-value.sparql")
    logger.info(query)    
    trans.translate(query)
  }
  "Filter Uri value query" should "be rewritten" in {     
    val query = loadQuery("queries/srbench/filter-uri-value.sparql")
    logger.info(query)    
    trans.translate(query)
  }

  
  
}