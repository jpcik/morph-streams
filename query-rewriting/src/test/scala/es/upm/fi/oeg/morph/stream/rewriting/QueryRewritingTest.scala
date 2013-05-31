package es.upm.fi.oeg.morph.stream.rewriting
import java.net.URI
import org.junit.Before
import org.junit.Test
import org.scalatest.junit.JUnitSuite
import org.scalatest.junit.ShouldMatchersForJUnit
import org.scalatest.prop.Checkers
import com.hp.hpl.jena.sparql.algebra.Algebra
import es.upm.fi.oeg.morph.common.ParameterUtils._
import es.upm.fi.oeg.sparqlstream.StreamQueryFactory
import es.upm.fi.oeg.morph.common.ParameterUtils
import org.junit.Ignore
import org.slf4j.LoggerFactory

class QueryRewritingTest extends JUnitSuite with ShouldMatchersForJUnit with Checkers {
  private val logger= LoggerFactory.getLogger(this.getClass)

  //PropertyConfigurator.configure(classOf[QueryRewritingTest].getClassLoader().getResource("config/log4j.properties"));
  val queryCCOComplexTide = loadQuery("queries/common/cco_queryPressure.sparql")
  val testQuery = loadQuery("queries/testQuery.sparql")
  val testQuerySimple = loadQuery("queries/testQuerySimple.sparql")
  val testQueryFilter = loadQuery("queries/testQueryFilter.sparql")
  val testQueryJoin = loadQuery("queries/testQueryJoin.sparql")
  val testQueryIsolate = loadQuery("queries/testQueryIsolate.sparql")
  val testConstructSimple = loadQuery("queries/testConstructSimple.sparql")
  val testConstructJoin = loadQuery("queries/testConstructJoin.sparql")
  val testConstruct = loadQuery("queries/testConstruct.sparql")
  val testConstructTide = loadQuery("queries/testConstructTide.sparql")

  val queryCCOWaveHeight = loadQuery("queries/queryCCOWaveHeight.sparql")
  val constructCCOWaveHeight = loadQuery("queries/constructCCOWaveHeight.sparql")

  val queryWannengratTemp = loadQuery("queries/wannengrat/queryTemp.sparql")
  val queryWannengratMetadataTemp = loadQuery("queries/wannengrat/queryMetadataTemp.sparql")
  val queryWannengratMetadataTempRemote = loadQuery("queries/wannengrat/queryMetadataTempRemote.sparql")
  val constructWannengratTemp = loadQuery("queries/wannengrat/constructTemp.sparql")
  val constructWannengratMetadataTemp = loadQuery("queries/wannengrat/constructMetadataTemp.sparql")

  val props = ParameterUtils.load(classOf[QueryRewritingTest].getClassLoader.getResourceAsStream("config/config_memoryStore.properties"));

  @Before def initialize() {}

  @Test def testTranslate() { //}throws QueryException, InvalidR2RDocumentException, InvalidR2RLocationException, URISyntaxException {
    logger.info(queryCCOComplexTide);
    val trans = new QueryRewriting(props,"mappings/cco.r2r");
    trans.translate(queryCCOComplexTide);
  }

  @Test def testAlgebraTransformation()  {
    logger.info(testQuery);
    val trans = new QueryRewriting(props,"mappings/cco.r2r");
    trans.translate(testQuery)
  }

  @Test
  def testParseNoStreams() {
    val queryString = "PREFIX fire: <http://www.semsorgrid4env.eu#> \n" +
      " PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> \n" +
      " SELECT ((?sid + ?sid * ?speed) AS ?tot) ?speed (seconds(?speed) AS ?popo) \n" +
      //" SELECT TIMESTAMP(?sid) AS ?pipo "+  
      //" SELECT (count(*) AS ?coso) ?direction ?sid ?name"+
      " WHERE" +
      " {" +
      " ?WindSpeed a fire:WindSpeedMeasurement;" +
      " fire:hasSpeed ?speed;" +
      " fire:isProducedBy ?Sensor." +
      " ?WindDirection a fire:WindDirectionMeasurement;" +
      " fire:hasDirection ?direction." +
      " ?Sensor a fire:Sensor;" +
      " fire:hasName ?name;" +
      " fire:hasSensorid ?sid." +
      " FILTER ( ?speed > 4 && ?speed <10)" +
      " FILTER ( ?speed < 10 )" +
      " FILTER ( ?direction = 3 )" +
      " }";

    val query = StreamQueryFactory.create(queryString);
    System.out.println(query.toString());

    val algebra = Algebra.compile(query);
    System.out.println(algebra);
  }

  @Test
  def testParse() {
    val queryString = "PREFIX fire: <http://www.semsorgrid4env.eu#>" +
      " PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>" +
      " SELECT ?speed ?direction ?sid ?name " +
      " FROM NAMED STREAM <http://www.ssg4env/mes.srdf> [NOW SLIDE 2 HOURS]" +
      " FROM NAMED STREAM <http://www.ssg4env/mes1.srdf> [NOW - 10 HOURS SLIDE 1 MINUTES]" +
      " FROM NAMED STREAM <http://www.ssg4env/mes2.srdf>" +
      " [NOW - 10 HOURS TO NOW - 0 HOURS SLIDE 1 MINUTES]" +
      " WHERE" +
      " {" +
      " ?WindSpeed a fire:WindSpeedMeasurement;" +
      " fire:hasSpeed ?speed;" +
      " fire:isProducedBy ?Sensor." +
      " ?WindDirection a fire:WindDirectionMeasurement;" +
      " fire:hasDirection ?direction." +
      " ?Sensor a fire:Sensor;" +
      " fire:hasName ?name;" +
      " fire:hasSensorid ?sid." +
      " FILTER ( ?speed > 4 && ?speed < 12 && ?direction >40)" +
      " FILTER ( ?speed < 10 )" +
      " FILTER ( ?direction = 3 )" +
      " }";

    val query = StreamQueryFactory.create(queryString);
    System.out.println("tic:" + query.toString());

  }

  @Test
  def testTranslateQuery() //throws  QueryTranslationException, URISyntaxException
  {    
    logger.info(testQuery);
    val trans = new QueryRewriting(props,"mappings/testMapping.r2r");
    trans.translate(testQuery);
  }

  @Test def testTranslateIsolatedQuery() {
    logger.info(testQueryIsolate);
    val trans = new QueryRewriting(props,"mappings/testMapping.r2r");
    trans.translate(testQueryIsolate);
  }

  @Test def testTranslateWithSQL {
    val trans = new QueryRewriting(props,"mappings/testMappingSQL.r2r");
    trans.translate(testQuerySimple)
  }

  @Test
  def testTranslateConstructSimple() //throws URISyntaxException, QueryTranslationException
  {
    val trans = new QueryRewriting(props,"mappings/testMapping.r2r");
    trans.translate(testConstructSimple);
  }

  @Test
  def testTranslateConstruct() //throws  URISyntaxException, QueryTranslationException
  {
    val trans = new QueryRewriting(props,"mappings/testMapping.r2r");
    trans.translate(testConstruct)
  }

  @Test
  def testTranslateFilters() //throws  URISyntaxException, QueryTranslationException
  {
    val trans = new QueryRewriting(props,"mappings/testMapping.r2r");
    trans.translate(testQueryFilter);
  }

  @Test
  def testTranslateJoin() //throws  URISyntaxException, QueryTranslationException
  {
    val trans = new QueryRewriting(props,"mappings/testMapping.r2r");
    trans.translate(testQueryJoin);
  }

  @Test
  def testTranslateCCOWaveHeight() //throws  URISyntaxException, QueryTranslationException
  {
    val trans = new QueryRewriting(props,"mappings/cco.r2r");
    trans.translate(queryCCOWaveHeight);
  }

  @Test
  def testTranslateWannengratTemp() //throws URISyntaxException, QueryTranslationException
  {
    val trans = new QueryRewriting(props,"mappings/wannengrat.r2r");
    trans.translate(queryWannengratTemp);
  }

  @Test
  def testTranslateWannengratMetadataTemp() //throws  URISyntaxException, QueryTranslationException
  {
    val trans = new QueryRewriting(props,"mappings/wannengrat.r2r");
    trans.translate(queryWannengratMetadataTemp);
  }

  @Test def testGraphStream()  {
    val trans = new QueryRewriting(props,"mappings/srbench.ttl");
    trans.translate(loadQuery("queries/srbench/graph-stream.sparql"))
  }

  @Test //@Ignore
  def testTranslateTwoWaves() //throws  URISyntaxException, QueryTranslationException, IOException
  {
    val trans = new QueryRewriting(props,"mappings/testMapping.r2r")
    trans.translate(loadQuery("queries/testQueryTwoWaves.sparql"))
  }

}