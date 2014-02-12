package es.upm.fi.oeg.morph.stream.rewriting
import java.net.URI
import org.scalatest.junit.JUnitSuite
import org.scalatest.junit.ShouldMatchersForJUnit
import org.scalatest.prop.Checkers
import com.hp.hpl.jena.sparql.algebra.Algebra
import es.upm.fi.oeg.morph.common.ParameterUtils._
import es.upm.fi.oeg.morph.common.ParameterUtils
import org.slf4j.LoggerFactory
import es.upm.fi.oeg.sparqlstream.SparqlStream
import es.upm.fi.oeg.morph.stream.query.SqlQuery
import org.scalatest.Matchers
import org.scalatest.FlatSpec

class QueryRewritingTest  extends FlatSpec with Matchers  {
  private val logger= LoggerFactory.getLogger(this.getClass)

  val transCCO = new QueryRewriting("mappings/cco.ttl","default")
  val trans = new QueryRewriting("mappings/testMapping.ttl","default")
  val transSQL = new QueryRewriting("mappings/testMappingSQL.ttl","default")
  val transWann = new QueryRewriting("mappings/wannengrat.ttl","default");
  val transSRBench = new QueryRewriting("mappings/srbench.ttl","default");
  
  def query(q:String)=loadQuery("queries/"+q+".sparql")

  "testQuery" should "be translated to a SQLQuery" in{
    val q=transCCO.translate(query("testQuery"))
    q shouldBe a [SqlQuery]
    q.projectionVars should contain allOf("wavets","waveheight","sensor")
    val sql=q.create
    sql.union shouldBe empty
    sql.projs.size shouldBe 3
    //sql.projs("wavets_DateTime").toString shouldBe "replace(pencode(DateTime),http://semsorgrid4env.eu/ns#Observation/WaveHeight/CCO/Instant/{DateTime})"
    //sql.projs("waveheight_DateTime").toString shouldBe "replace(pencode(DateTime),http://semsorgrid4env.eu/ns#Observation/WaveHeight/CCO/Output/{DateTime})"
    sql.projs("sensor").toString shouldBe "'http://es.upm.fi.dia.oeg/R2RMapping#milfordSensor'"
    sql.sels shouldBe empty
    sql.from.size shouldBe 1
  //  val stream=sql.from("envdata_milford")
  //  stream.name shouldBe "envdata_milford"
  //  stream.window shouldBe None
    //projection (wavets -> replace(pencode(DateTime),http://semsorgrid4env.eu/ns#Observation/WaveHeight/CCO/Instant/{DateTime}),waveheight -> replace(pencode(DateTime),http://semsorgrid4env.eu/ns#Observation/WaveHeight/CCO/Output/{DateTime}),sensor -> 'http://es.upm.fi.dia.oeg/R2RMapping#milfordSensor')
	//	relation envdata_milford
  }

  "cco query" should "be translated" in{    
    val q=transCCO.translate(query("queryCCOWaveHeight"))
    val sql=q.create
    sql.union shouldBe empty
    logger.debug("from "+sql.from)
    sql.from.size shouldBe 2
    sql.from.values.map(_.name) should contain allOf ("envdata_milford","const")
    sql.sels.size shouldBe 1
    logger.debug("cond "+sql.sels)
    
  }
  /*projection (wavets -> ?,waveheight -> ?,WaveObs -> ?)
      join (feature = feature)
	    projection (wavets -> timestamp,feature -> 'http://semsorgrid4env.eu/ns#Sea',result -> replace(pencode(DateTime),http://semsorgrid4env.eu/ns#Observation/WaveHeight/CCO/Output/{DateTime}),waveheight -> Hs,WaveObs -> replace(pencode(DateTime),http://semsorgrid4env.eu/ns#Observation/WaveHeight/CCO/{DateTime}),instant -> replace(pencode(DateTime),http://semsorgrid4env.eu/ns#Observation/WaveHeight/CCO/Instant/{DateTime}),value -> replace(pencode(DateTime),http://semsorgrid4env.eu/ns#ObservationValue/WaveHeight/CCO/{DateTime}))
		  relation envdata_milford
 		projection (feature -> 'http://semsorgrid4env.eu/ns#Sea')
		  relation const  */


  "testQuery" should "be translated" in{    
    val q=trans.translate(query("testQuery"))
    val sql=q.create
    sql.from shouldBe empty
    sql.projs shouldBe empty
    sql.sels shouldBe empty
    sql.union.size shouldBe 3
    sql.union.map(_.from.map(_._2.name)).flatten should contain allOf ("envdata_folkestone","envdata_hernebay","envdata_milford")
  }
  /*
   multiunion
     projection (wavets -> timestamp,waveheight -> Hs,sensor -> 'http://www.semsorgrid4env.eu/ontologies/CoastalDefences.owl#FolkestoneSensor')
       relation envdata_folkestone
     projection (wavets -> timestamp,waveheight -> Hs,sensor -> 'http://www.semsorgrid4env.eu/ontologies/CoastalDefences.owl#HernebaySensor')
       relation envdata_hernebay
     projection (wavets -> timestamp,waveheight -> Hs,sensor -> 'http://www.semsorgrid4env.eu/ontologies/CoastalDefences.owl#MilfordSensor')
       relation envdata_milford
   */

  "testQueryIsolate" should "be translated" in{    
    trans.translate(query("testQueryIsolate"))
  }


  "Construct query" should "be translated" in {
    trans.translate(query("testConstructSimple"))
  }

  "Complex construct query" should "be translated" in{
    trans.translate(query("testConstruct"))
  }

  "Filter Query" should "be translated" in{
    trans.translate(query("testQueryFilter"))
  }

  "Query with two streams" should "be translated" in {
    trans.translate(query("testQueryTwoWaves"))
  }
  
  "Query with join" should "be translated" in {
    trans.translate(query("testQueryJoin"))
  }
  
  "Query with SQL in mapping" should "be translated" in {
    transSQL.translate(query("testQuerySimple"))
  }

  "Wannengrat Query" should "be translated" in {    
    transWann.translate(query("wannengrat/queryTemp"))
  }

  "Wannengrat metadata Query" should "be translated" in {
    transWann.translate(query("wannengrat/queryMetadataTemp"))
  }

  "Query with graph stream" should "be translated" in {
    val q=transSRBench.translate(query("srbench/graph-stream"))
    println(q.serializeQuery)
  }


}