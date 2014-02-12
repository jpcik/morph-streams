package es.upm.fi.oeg.sparqlstream

import es.upm.fi.oeg.morph.common.ParameterUtils._
import org.slf4j.LoggerFactory
import org.scalatest.FlatSpec
import org.scalatest.Matchers


class SparqlStreamParseTest extends FlatSpec with Matchers  {
  private val logger= LoggerFactory.getLogger(this.getClass)
  def query(name:String)=loadQuery("queries/"+name+".sparql")

  "Query with no streams" should "parse no streams" in{
    val q=SparqlStream.parse(query("testQuery"))
    logger.debug(q.serialize)
    q.streams.size should be(0)
  }
  
  "Query with from stream" should "parse named stream" in{
    val q=SparqlStream.parse(query("testQueryJoin"))
    q.streams.contains("http://semsorgrid4env.eu/ns#ccometeo.srdf") should be (true)
  }

  "Query with dstream" should "parse dstream" in{
    val q=SparqlStream.parse(query("testQueryDstream"))
    q.streams.contains("http://semsorgrid4env.eu/ns#ccometeo.srdf") should be (true)
    logger.debug(""+q.r2s)
    q.isDstream should be (true)
  }
}