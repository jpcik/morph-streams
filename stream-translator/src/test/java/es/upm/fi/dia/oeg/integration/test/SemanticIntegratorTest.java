package es.upm.fi.dia.oeg.integration.test;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;


import org.apache.log4j.Logger;
import org.codehaus.plexus.util.FileUtils;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;


import com.google.common.collect.Lists;

import es.upm.fi.dia.oeg.common.ParameterUtils;
import es.upm.fi.dia.oeg.integration.DataSourceException;
import es.upm.fi.dia.oeg.integration.IntegratorConfigurationException;
import es.upm.fi.dia.oeg.integration.SemanticIntegrator;
import es.upm.fi.dia.oeg.integration.metadata.DataSourceMetadata;
import es.upm.fi.dia.oeg.integration.metadata.IntegratedDataSourceMetadata;
import es.upm.fi.dia.oeg.integration.metadata.MappingDocumentMetadata;
import es.upm.fi.dia.oeg.integration.metadata.PullDataSourceMetadata;
import es.upm.fi.dia.oeg.integration.metadata.SPARQLServiceMetadata;
import es.upm.fi.dia.oeg.integration.metadata.SourceType;
import es.upm.fi.dia.oeg.integration.metadata.mappings.MappingLanguage;
import es.upm.fi.dia.oeg.integration.registry.FileIntegratorRegistry;
import es.upm.fi.dia.oeg.integration.registry.IntegratorRegistryException;
import es.upm.fi.dia.oeg.integration.translation.QueryTranslator;
import es.upm.fi.dia.oeg.morph.r2rml.InvalidR2RDocumentException;

public class SemanticIntegratorTest extends QueryTestBase 
{
	
    @Rule
    public static TemporaryFolder folder = new TemporaryFolder();
    
	protected static SemanticIntegrator si;
	private static QueryTranslator qt;
	protected static Properties props;
	public static final String SPARQL_STREAMING_QUERY_FILE_PATH = "s2o.test.sparql.streaming.query.file.path";
	protected static Logger logger = Logger.getLogger(SemanticIntegratorTest.class.getName());
	
	@BeforeClass
	public static void setUpBeforeClass() throws IOException, URISyntaxException, IntegratorRegistryException, IntegratorConfigurationException
	{
		File tempFolder = folder.newFolder("mappings");
		//PropertyConfigurator.configure(SemanticIntegratorTest.class.getClassLoader().getResource("config/log4j.properties"));
		init();
		FileUtils.cleanDirectory(tempFolder);
		FileUtils.fileAppend(tempFolder.getAbsolutePath()+"/mappings.xml", 
				"<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?>" +
				"<mappings xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\">"+
				"<virtualSources/></mappings>");

		props = ParameterUtils.load(SemanticIntegratorTest.class.getClassLoader().getResourceAsStream("config/config_memoryStore.properties"));
		//props.setProperty(FileIntegratorRegistry.INTEGRATOR_REPOSITORY_URL, tempFolder.toURI().toURL().toString());
		
		si = new SemanticIntegrator(props);
		
	

	}

	protected InputStream loadTestMappingModel() throws InvalidR2RDocumentException
	{
		//R2RModel r2r = new R2RModel();
		InputStream is = SemanticIntegratorTest.class.getClassLoader().getResourceAsStream("mappings/cco.r2r");
		
		//r2r.read(is);
		return is;
	}
	
	
			
		
	@Test @Ignore
	public void testRemoveAllPullDataSources() throws DataSourceException, IntegratorRegistryException
	{
		si.removeAllPullDataSources();
	}
	
	@Test
	public void testRetrieveIntegratedResource() throws DataSourceException
	{
		IntegratedDataSourceMetadata isMetadata = si.retrieveIntegratedDataSource("urn:ssg4e:iqs:GeneratorWave");
		
		SPARQLServiceMetadata desc = isMetadata.getServiceDescription();
		logger.debug("Dataset: "+desc.getDefaultDataset());
		logger.debug("Uri "+ desc.getServiceUri());
		logger.debug("Language "+desc.getSupportedLanguage());
		logger.debug("Url "+desc.getServiceUrl());
		logger.debug("Doc "+desc.getDocument());
		logger.debug("Name "+isMetadata.getSourceName());
		logger.debug("MappingLang "+isMetadata.getMapping().getLanguage());
		logger.debug("Mapping uri "+isMetadata.getMapping().getName());
		for (DataSourceMetadata ds: isMetadata.getSourceList())
		{
			logger.debug("source: "+ds.getSourceName()+ " "+ds.getUri()+" "+ds.getType());
		}
	}
	
	@Test
	public void testIntegrateAsLocal() throws DataSourceException, URISyntaxException
	{
		List<DataSourceMetadata> sources = Lists.newArrayList();
		sources.add(new DataSourceMetadata("dsfsdfs", SourceType.SERVICE, 
				new URI("http://webgis1.geodata.soton.ac.uk:8080/CCO/services/PullStream?wsdl")));
		MappingDocumentMetadata mapping = new MappingDocumentMetadata("mappingnamin",MappingLanguage.R2RML,new URI("sdfdsfsd"));
		mapping.setMapping(this.getClass().getClassLoader().getResourceAsStream("mappings/testMapping.r2r"));
		si.integrateAs(sources , "testnewIntegratedResource", mapping );
	}
	
}
