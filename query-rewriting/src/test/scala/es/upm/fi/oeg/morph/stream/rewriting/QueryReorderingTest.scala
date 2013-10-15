package es.upm.fi.oeg.morph.stream.rewriting

import org.scalatest.junit.ShouldMatchersForJUnit
import org.scalatest.prop.Checkers
import org.scalatest.junit.JUnitSuite
import org.slf4j.LoggerFactory
import org.junit.Test
import es.upm.fi.oeg.sparqlstream.SparqlStream

class QueryReorderingTest extends JUnitSuite with ShouldMatchersForJUnit with Checkers {
  private val logger= LoggerFactory.getLogger(this.getClass)

  private val q1="""PREFIX sr4ld: <http://streamreasoning.org/ontologies/social#>
          PREFIX pers: <http://streamreasoning.org/data/person/id/>
          PREFIX room: <http://streamreasoning.org/data/room/id/>
          SELECT ?obs ?obs1 
          FROM NAMED STREAM <http://streamreasoning.org/data/social.srdf> [NOW - 30 S]
          WHERE {
            ?obs1 sr4ld:who pers:bob.
            ?obs  sr4ld:where room:r1.
            ?obs1  sr4ld:where room:r2.
          }"""
  
  private def reorder(query:String)={
    QueryReordering.reorder(SparqlStream.parse(query))
  }
  @Test def reorderSubject={
    logger.debug("reordered: "+reorder(q1))
  }

}