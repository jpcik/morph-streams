package es.upm.fi.dia.oeg.integration.adapter.snee.test;


import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import uk.ac.manchester.cs.snee.SNEEException;
import uk.ac.manchester.cs.snee.datasource.webservice.PullStreamServiceClient;
import uk.ac.manchester.cs.snee.metadata.schema.ExtentDoesNotExistException;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;

import es.upm.fi.dia.oeg.integration.DataSourceException;
import es.upm.fi.dia.oeg.integration.QueryException;
import es.upm.fi.dia.oeg.integration.adapter.snee.SNEEAdapter;
import es.upm.fi.dia.oeg.integration.metadata.SourceType;
import eu.semsorgrid4env.service.stream.pull.GetStreamItemRequest;
import eu.semsorgrid4env.service.stream.pull.InvalidCountFault;
import eu.semsorgrid4env.service.stream.pull.InvalidPositionFault;
import eu.semsorgrid4env.service.stream.pull.MaximumTuplesExceededFault;
import eu.semsorgrid4env.service.stream.pull.PullStreamInterface;
import eu.semsorgrid4env.service.stream.pull.PullStreamService;
import eu.semsorgrid4env.service.wsdai.DataResourceUnavailableFault;
import eu.semsorgrid4env.service.wsdai.GenericQueryResponse;
import eu.semsorgrid4env.service.wsdai.GetDataResourcePropertyDocumentRequest;
import eu.semsorgrid4env.service.wsdai.GetResourceListRequest;
import eu.semsorgrid4env.service.wsdai.GetResourceListResponse;
import eu.semsorgrid4env.service.wsdai.InvalidDatasetFormatFault;
import eu.semsorgrid4env.service.wsdai.InvalidResourceNameFault;
import eu.semsorgrid4env.service.wsdai.NotAuthorizedFault;
import eu.semsorgrid4env.service.wsdai.PropertyDocumentType;
import eu.semsorgrid4env.service.wsdai.ServiceBusyFault;

public class SNEEAdapterIT
{

	private static Logger logger = Logger.getLogger(SNEEAdapterIT.class.getName());

	static SNEEAdapter adap = new SNEEAdapter();
	@BeforeClass
	public static void setUpBeforeClass() throws Exception
	{
		PropertyConfigurator.configure(SNEEAdapterIT.class.getClassLoader().getResource("config/log4j.properties"));
		adap.init("snee/ws/snee.properties");
	}
	

	@Test
	public void testAddSource() throws MalformedURLException, DataSourceException
	{
		//adap.addPullSource("http://webgis1.geodata.soton.ac.uk:8080/CCO/services/PullStream?wsdl");		
	}
	
	@Test@Ignore
	public void testQuery() throws QueryException, MalformedURLException, DataSourceException, InterruptedException, SNEEException, SQLException
	{
		adap.addPullSource("http://webgis1.geodata.soton.ac.uk:8080/CCO/services/PullStream?wsdl",SourceType.SERVICE);
		//adap.addPullSource("http://webgis1.geodata.soton.ac.uk:8080/WaveNet/services/PullStream?wsdl");
		//adap.addPullSource("http://webgis1.geodata.soton.ac.uk:8080/EMU/services/PullStream?wsdl",SourceType.SERVICE);
		//String queryString = " SELECT Hs, DateTime FROM bidefordbay_wave;";//[FROM NOW - 1 HOUR TO NOW SLIDE 1 MINUTE];";
		String queryString = "(SELECT 'envdata_torbay' AS extentname, Hs AS waveheight, DateTime AS wavets " +
				"FROM envdata_torbay) UNION  (SELECT 'envdata_bidefordbay' AS extentname, " +
				"Hs AS waveheight, DateTime AS wavets FROM envdata_bidefordbay) UNION " +
				"(SELECT 'envdata_goodwin' AS extentname, Hs AS waveheight, DateTime AS " +
				"wavets FROM envdata_goodwin) UNION  (SELECT 'envdata_milford' AS " +
				"extentname, Hs AS waveheight, DateTime AS wavets FROM envdata_milford) " +
				"UNION  (SELECT 'envdata_pevenseybay' AS extentname, Hs AS waveheight, " +
				"DateTime AS wavets FROM envdata_pevenseybay) UNION  (SELECT " +
				"'envdata_rustington' AS extentname, Hs AS waveheight, DateTime AS wavets " +
				"FROM envdata_rustington) UNION  (SELECT 'envdata_perranporth' AS " +
				"extentname, Hs AS waveheight, DateTime AS wavets FROM envdata_perranporth);";
		String id = adap.invokeQueryFactory(queryString , 5);
		
		Thread.sleep(5000);
		
		List<ResultSet> list =adap.pullData(id);
		SNEEAdapter.printResults(list);
	}
	
	@Test@Ignore
	public void testQueryBoscombe() throws QueryException, MalformedURLException, InterruptedException, SNEEException, SQLException, ExtentDoesNotExistException, SchemaMetadataException, InvalidResourceNameFault, DataResourceUnavailableFault, NotAuthorizedFault, ServiceBusyFault, URISyntaxException, MaximumTuplesExceededFault, InvalidPositionFault, InvalidCountFault, InvalidDatasetFormatFault
	{
		
		PullStreamServiceClient cli = new PullStreamServiceClient("http://ssg4e.techideas.net:8180/ABP/services/PullStream?wsdl");
		
		PullStreamService srv = new PullStreamService(new URI("http://ssg4e.techideas.net:8180/AIS/services/PullStream?wsdl").toURL());
		PullStreamInterface pull = srv.getPullStreamInterface();
		GetStreamItemRequest req = new GetStreamItemRequest();
		req.setDataResourceAbstractName("ais:pull:Southampton_Boat");
		
		GenericQueryResponse resp = pull.getStreamItem(req );
		System.out.println(resp.getDataset().getDatasetData().getContent().get(0));
		/*
		GetDataResourcePropertyDocumentRequest req = new GetDataResourcePropertyDocumentRequest();
		
		req.setDataResourceAbstractName("abp:pull:Bramblemet_met");
		GetResourceListRequest req1 = new GetResourceListRequest();
		GetResourceListResponse list1 = pull.getResourceList(req1 );
		list1.getDataResourceAddress().size();
		pull.getDataResourcePropertyDocument(req );*/
		
		try
		{
		//adap.addPullSource("http://webgis1.geodata.soton.ac.uk:8080/EMU/services/PullStream?wsdl",SourceType.SERVICE);

		//adap.addPullSource("http://webgis1.geodata.soton.ac.uk:8080/dai/services/AccessServiceCoreDataAccessPT?wsdl", SourceType.WSDAIR);
		//adap.addPullSource("http://webgis1.geodata.soton.ac.uk:8080/dai/services/AccessServiceAccessFactoryPT?wsdl",SourceType.SERVICE);
		//adap.addPullSource("http://ssg4e.techideas.net:8180/ABP/services/PullStream?wsdl",SourceType.SERVICE);
		//adap.addPullSource("http://ssg4e.techideas.net:8180/AIS/services/PullStream?wsdl",SourceType.SERVICE);
		adap.addPullSource("http://webgis1.geodata.soton.ac.uk:8080/CCO/services/PullStream?wsdl",SourceType.SERVICE);
		//adap.addPullSource("http://localhost:8080/CCO/services/PullStream?wsdl",SourceType.SERVICE);
		//adap.addPullSource("http://localhost:8080/CCO/services/PullStream?wsdl",SourceType.SERVICE);
		}
		catch (DataSourceException e)
		{
			e.printStackTrace();
			throw new RuntimeException("bad");
		}
		//String queryString = " SELECT Observed,Hs,Tz FROM envdata_deal_tide;";//[FROM NOW - 1 HOUR TO NOW SLIDE 1 MINUTE];";
		//String queryString = " SELECT * FROM rtdata_Chesil;";//[FROM NOW - 1 HOUR TO NOW SLIDE 1 MINUTE];";
		//String queryString = "(SELECT 'envdata_torbay' AS extentname, Hs AS waveheight, Tz AS papas, DateTime AS wavets  " +
		//		"FROM envdata_boscombe) ;";
		//adap.getDetails("rtdata_goodwin");
		//adap.getDetails("envdata_chichesterharbour");
		//adap.getDetails("shipdata_southampton");
		//String queryString = " SELECT wind_gust_speed FROM envdata_southampton;";
		//String queryString = " SELECT * FROM shipdata_southampton;";
		String queryString = " SELECT Hs+100.4 as coso FROM envdata_milford;";
		//String queryString = " SELECT * FROM rtdata_goodwin;";
		String id = adap.invokeQueryFactory(queryString , 5);
		logger.debug("Query id "+id);
		Thread.sleep(10000);
		
		List<ResultSet> list =adap.pullNewestData(id);
		//Thread.sleep(1000000);
		
		list =adap.pullNewestData(id);
		SNEEAdapter.printResults(list);
	}
	
}
