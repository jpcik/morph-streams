package es.upm.fi.oeg.morph.stream.rewriting

import org.slf4j.LoggerFactory
import es.upm.fi.oeg.sparqlstream.SparqlStream
import org.scalatest.FlatSpec
import org.scalatest.Matchers

class QueryReorderingTest extends FlatSpec with Matchers {
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
  
  private val q2="""PREFIX om-owl: <http://knoesis.wright.edu/ssw/ont/sensor-observation.owl#>
         PREFIX weather: <http://knoesis.wright.edu/ssw/ont/weather.owl#>
         PREFIX ssn: <http://purl.oclc.org/NET/ssnx/ssn#>
         PREFIX qu: <http://purl.oclc.org/NET/ssnx/qu/qu#>
         SELECT ?observation 
         FROM NAMED STREAM <http://streamreasoning.org/data/social.srdf> [NOW - 30 S]
         WHERE { 
      { ?obsvalue <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.oclc.org/NET/ssnx/cf/cf-feature#Humidity> .
        ?obsvalue qu:numericalValue ?value .
        ?observation ssn:featureOfInterest ?obsvalue .
        ?result ssn:hasValue ?obsvalue .
        ?observation ssn:observationResult ?result}
    UNION
      { ?obsvalue qu:numericalValue ?value .
        ?observation <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ssn:Property .
        ?result ssn:hasValue ?obsvalue .
        ?observation ssn:observationResult ?result .
        ?observation ssn:observedProperty ?observation}  
}"""
  private def reorder(query:String)={
    QueryReordering.reorder(SparqlStream.parse(query))
  }
  "q1" should "be reordered by subject" in{
    logger.debug("reordered: "+reorder(q1))
  }

  "q2 union query" should "be reordered in 2 parts" in{
    logger.debug("reordered: "+reorder(q2))
  }

}