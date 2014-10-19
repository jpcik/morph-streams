package es.upm.fi.oeg.morph.streams.esper

import org.scalatest.junit.JUnitSuite
import org.scalatest.prop.Checkers
import org.slf4j.LoggerFactory
import org.scalatest.junit.ShouldMatchersForJUnit
import es.upm.fi.oeg.siq.tools.ParameterUtils
import java.net.URI
import es.upm.fi.oeg.sparqlstream.SparqlStream
import es.upm.fi.oeg.morph.stream.rewriting.QueryRewriting
import es.upm.fi.oeg.morph.stream.esper.EsperQuery
import org.scalatest.Matchers
import org.scalatest.FlatSpec
import org.scalatest.BeforeAndAfterAll

class QueryGenerationTest extends FlatSpec with BeforeAndAfterAll with Matchers   {
  private val logger= LoggerFactory.getLogger(this.getClass)
  //val props = ParameterUtils.load(getClass.getClassLoader.getResourceAsStream("config/siq.properties"))
  
  private def srbench(q:String)=ParameterUtils.loadQuery("queries/srbench/"+q)
  private val srbenchR2rml=new URI("mappings/srbench.ttl")
  
  private def rewrite(sparqlstr:String)={    
    val trans = new QueryRewriting(srbenchR2rml.toString,"esper")
    trans.translate(SparqlStream.parse(sparqlstr)).asInstanceOf[EsperQuery]
  }
    
  override def beforeAll()= {    
    println("finish init")
  }
  

  "filterUriDiff" should "filter" in{    
    val q=rewrite(srbench("filter-uri-diff.sparql"))
    q.serializeQuery should be ("SELECT DISTINCT " +
    		"rel0.stationId AS observation_stationId," +
    		"rel0.observationTime AS observation_observationTime " +
    		"FROM " +
    		"wunderground.win:time(1.0 hour) AS rel1," +
    		"wunderground.win:time(1.0 hour) AS rel0 " +
    		"WHERE ('http://meteo.us/' || rel0.stationId || '/temperature/observation/' || cast(rel0.observationTime,string)) != ('http://meteo.us/' || rel1.stationId || '/humidity/observation/' || cast(rel1.observationTime,string))  " +
    		"output snapshot every 0.98 hour")
  }

  "joinPatternObjects" should "join" in{
    
    val q=rewrite(srbench("join-pattern-objects.sparql"))   
    val res=Array("SELECT DISTINCT " +
    		"rel1.stationId AS observation_stationId," +
    		"rel0.observationTime AS observation2_observationTime," +
    		"rel0.stationId AS observation2_stationId," +
    		"rel1.stationId AS sensor_stationId," +
    		"rel1.observationTime AS observation_observationTime " +
    		"FROM " +
    		"wunderground.win:time(1.0 hour) AS rel0," +
    		"wunderground.win:time(1.0 hour) AS rel1 " +
    		"WHERE " +
    		"rel0.stationId=rel1.stationId  " +
    		"output snapshot every 0.98 hour",
    		"SELECT DISTINCT " +
    		"rel1.stationId AS observation_stationId," +
    		"rel0.observationTime AS observation2_observationTime," +
    		"rel0.stationId AS observation2_stationId," +
    		"rel1.stationId AS sensor_stationId," +
    		"rel1.observationTime AS observation_observationTime " +
    		"FROM " +
    		"wunderground.win:time(1.0 hour) AS rel0," +
    		"wunderground.win:time(1.0 hour) AS rel1 " +
    		"WHERE " +
    		"rel0.stationId=rel1.stationId  " +
    		"output snapshot every 0.98 hour",
    		"SELECT DISTINCT " +
    		"rel1.stationId AS observation_stationId," +
    		"rel0.observationTime AS observation2_observationTime," +
    		"rel0.stationId AS observation2_stationId," +
    		"rel1.stationId AS sensor_stationId," +
    		"rel1.observationTime AS observation_observationTime " +
    		"FROM " +
    		"wunderground.win:time(1.0 hour) AS rel0," +
    		"wunderground.win:time(1.0 hour) AS rel1 " +
    		"WHERE " +
    		"rel0.stationId=rel1.stationId  " +
    		"output snapshot every 0.98 hour",
    		"SELECT DISTINCT " +
    		"rel1.stationId AS observation_stationId," +
    		"rel0.observationTime AS observation2_observationTime," +
    		"rel0.stationId AS observation2_stationId," +
    		"rel1.stationId AS sensor_stationId," +
    		"rel1.observationTime AS observation_observationTime " +
    		"FROM " +
    		"wunderground.win:time(1.0 hour) AS rel0," +
    		"wunderground.win:time(1.0 hour) AS rel1 " +
    		"WHERE " +
    		"rel0.stationId=rel1.stationId  " +
    		"output snapshot every 0.98 hour")
    q.unions.foreach(a=>println(a))
    compare(q,res) should be (true)
  }

  "basicPatternMatching" should "match" in{    
    val q=rewrite(srbench("basic-pattern-matching.sparql"))
    q.serializeQuery should be ("SELECT DISTINCT " +
    		"'http://oeg-upm.net/ns/morph#celsius' AS uom," +
    		"rel0.temperature AS value," +
    		"rel0.stationId AS sensor_stationId " +
    		"FROM " +
    		"wunderground.win:time(10.0 second) AS rel0  " +
    		"output snapshot every 0.98 second")
  }
    
  "filterValue" should "filter" in{ 	 
    val q=rewrite(srbench("filter-value.sparql"))
    val res= Array("SELECT DISTINCT " +
    		"rel0.stationId AS sensor_stationId " +
    		"FROM " +
    		"wunderground.win:time(1.0 hour) AS rel0 " +
    		"WHERE " +
    		"rel0.temperature > 0.3 AND rel0.temperature < 0.7  " +
    		"output snapshot every 0.98 hour",
    		"SELECT DISTINCT " +
    		"rel0.stationId AS sensor_stationId " +
    		"FROM " +
    		"wunderground.win:time(1.0 hour) AS rel0 " +
    		"WHERE rel0.windSpeed > 0.3 AND rel0.windSpeed < 0.7  " +
    		"output snapshot every 0.98 hour")
    compare(q,res) should be (true)
  }    
  
  "joinPatternMatching" should "join" in{ 	 
    val q=rewrite(srbench("join-pattern-matching.sparql"))        
    logger.info(q.serializeQuery)
    val res=Array("SELECT DISTINCT " +
    		"NULL AS sensor," +
    		"rel0.stationId AS observation_stationId," +
    		"rel0.relativeHumidity AS value," +
    		"rel0.observationTime AS observation_observationTime " +
    		"FROM " +
    		"wunderground.win:time(1.0 hour) AS rel0  " +
    		"output snapshot every 0.98 hour",
    		"SELECT DISTINCT " +
    		"NULL AS sensor," +
    		"rel0.stationId AS observation_stationId," +
    		"rel0.temperature AS value," +
    		"rel0.observationTime AS observation_observationTime " +
    		"FROM " +
    		"wunderground.win:time(1.0 hour) AS rel0  " +
    		"output snapshot every 0.98 hour")
    compare(q,res) should be (true)
  }    

  "optionalPatternMatching" should "optional" in{ 	 
    val q=rewrite(srbench("optional-pattern-matching.sparql"))
    val res=Array("SELECT DISTINCT " +
    		"'http://oeg-upm.net/ns/morph#celsius' AS uom," +
    		"NULL AS sensor," +
    		"rel0.observationTime AS result_observationTime," +
    		"rel0.stationId AS result_stationId," +
    		"rel0.temperature AS value " +
    		"FROM " +
    		"wunderground.win:time(1.0 hour) AS rel0  " +
    		"output snapshot every 0.98 hour",
    		"SELECT DISTINCT " +
    		"'http://oeg-upm.net/ns/morph#percentage' AS uom," +
    		"NULL AS sensor," +
    		"rel0.observationTime AS result_observationTime," +
    		"rel0.stationId AS result_stationId," +
    		"rel0.relativeHumidity AS value " +
    		"FROM " +
    		"wunderground.win:time(1.0 hour) AS rel0  " +
    		"output snapshot every 0.98 hour")
    compare(q,res) should be (true)
  }    

  "optionalJoinObservations" should "optional" in{ 	 
    val q=rewrite(srbench("optional-join-observations.sparql"))
    q.serializeQuery should be ("SELECT DISTINCT " +
    		"rel1.stationId AS observation_stationId," +
    		"rel0.observationTime AS observation2_observationTime," +
    		"rel0.stationId AS observation2_stationId," +
    		"rel1.stationId AS sensor_stationId," +
    		"rel1.observationTime AS observation_observationTime "+
    		"FROM " +
    		"wunderground.win:time(2.0 second) AS rel0," +
    		"wunderground.win:time(2.0 second) AS rel1 " +
    		"WHERE " +
    		"rel0.stationId=rel1.stationId  " +
    		"output snapshot every 0.98 second")
  }    

  "filterUriValue" should "filter" in{ 	 
    val q=rewrite(srbench("filter-uri-value.sparql"))
    q.serializeQuery should be ("SELECT DISTINCT " +
    		"rel0.temperature AS value," +
    		"rel0.stationId AS sensor_stationId " +
    		"FROM " +
    		"wunderground.win:time(1.0 hour) AS rel0 " +
    		"WHERE " +
    		"rel0.temperature > 0.5  " +
    		"output snapshot every 0.98 hour")
  }    

  "filterUriInstance" should "filter" in{ 	 
    val q=rewrite(srbench("filter-uri-instance.sparql"))
    logger.debug("query "+q.serializeQuery)
    /*q.serializeQuery should be ("SELECT DISTINCT " +
    		"rel0.temperature AS value," +
    		"rel0.stationId AS sensor_stationId " +
    		"FROM " +
    		"wunderground.win:time(1.0 hour) AS rel0 " +
    		"WHERE " +
    		"rel0.temperature > 0.5  " +
    		"output snapshot every 0.98 hour")*/
  }    

  
  "variablePredicate" should "var" in{ 	 
    val q=rewrite(srbench("variable-predicate.sparql"))
    logger.info(q.serializeQuery)
    val res=Array("SELECT DISTINCT NULL AS sensor,rel0.observationTime AS result_observationTime," +
    		"rel0.stationId AS observation_stationId,rel0.stationId AS result_stationId,rel0.temperature AS value," +
    		"rel0.observationTime AS observation_observationTime " +
    		"FROM wunderground.win:time(1.0 hour) AS rel0  output snapshot every 0.98 hour",
    		"SELECT DISTINCT NULL AS sensor,rel0.observationTime AS result_observationTime," +
    		"rel0.stationId AS observation_stationId,rel0.stationId AS result_stationId,rel0.relativeHumidity AS value," +
    		"rel0.observationTime AS observation_observationTime " +
    		"FROM wunderground.win:time(1.0 hour) AS rel0  output snapshot every 0.98 hour",
    		"SELECT DISTINCT NULL AS sensor,rel0.observationTime AS result_observationTime," +
    		"rel0.stationId AS observation_stationId,rel0.stationId AS result_stationId," +
    		"'http://oeg-upm.net/ns/morph#percentage' AS value,rel0.observationTime AS observation_observationTime " +
    		"FROM wunderground.win:time(1.0 hour) AS rel0  output snapshot every 0.98 hour",
    		"SELECT DISTINCT NULL AS sensor,rel0.observationTime AS result_observationTime," +
    		"rel0.stationId AS observation_stationId,rel0.stationId AS result_stationId," +
    		"'http://oeg-upm.net/ns/morph#celsius' AS value,rel0.observationTime AS observation_observationTime " +
    		"FROM wunderground.win:time(1.0 hour) AS rel0  output snapshot every 0.98 hour")
    compare(q,res) should be (true)
  }    

  "maxAggregate" should "agg" in{ 	 
    val q=rewrite(srbench("max-aggregate.sparql"))  
    q.serializeQuery should be ("SELECT " +
    		"max(rel0.temperature) AS maxi " +
    		"FROM " +
    		"wunderground.win:time(1.0 hour) AS rel0 " +
    		"WHERE " +
    		"rel0.temperature < 0.3  " +
    		"output snapshot every 0.98 hour")
  }    

  "countAggregate" should "count" in{ 	 
    val q=rewrite(srbench("count-aggregate.sparql"))  
    q.serializeQuery should be ("SELECT " +
    		"max(rel0.temperature) AS maxi " +
    		"FROM " +
    		"wunderground.win:time(1.0 hour) AS rel0 " +
    		"WHERE " +
    		"rel0.temperature < 0.3  " +
    		"output snapshot every 0.98 hour")
  }    
  
  
  "staticJoin" should "join" in{ 	 
    val q=rewrite(srbench("static-join.sparql"))        
  }    


  private def compare(q:EsperQuery,union:Array[String])={
    if (q.unionQueries.isEmpty) false
    else {
      val sq=q.unionQueries.map(_.serializeQuery)
      union.forall(u=>sq.contains(u))
    }
  }
  
}