package es.upm.fi.dia.oeg.integration.test;

import static org.junit.Assert.*;

import java.io.File;
import java.io.InputStream;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Rule;

import org.junit.Test;
import org.junit.rules.TemporaryFolder;


import es.upm.fi.dia.oeg.common.ParameterUtils;
import es.upm.fi.dia.oeg.integration.DataSourceException;
import es.upm.fi.dia.oeg.integration.metadata.IntegratedDataSourceMetadata;
import es.upm.fi.dia.oeg.integration.metadata.MappingDocumentMetadata;
import es.upm.fi.dia.oeg.integration.metadata.mappings.MappingLanguage;
import es.upm.fi.dia.oeg.integration.registry.FileIntegratorRegistry;
import es.upm.fi.dia.oeg.integration.registry.IntegratorRegistryException;

public class FileIntegratorRegistryTest
{
	private static Logger logger = Logger.getLogger(FileIntegratorRegistryTest.class.getName());
	static FileIntegratorRegistry fileRegistryRelative;
	static FileIntegratorRegistry fileRegistry;
	
    @Rule
    public static TemporaryFolder folder = new TemporaryFolder();
    
	@BeforeClass
	public static void init() throws Exception
	{
		PropertyConfigurator.configure(FileIntegratorRegistryTest.class.getClassLoader().getResource("config/log4j.properties"));

		File tempFolder = folder.newFolder("mappings");
		Properties props = ParameterUtils.load(FileIntegratorRegistryTest.class.getClassLoader().getResourceAsStream("config/config_memoryStore.properties"));
		//props.setProperty(FileIntegratorRegistry.INTEGRATOR_REPOSITORY_URL, folder.getRoot().getAbsolutePath());
		fileRegistryRelative = new FileIntegratorRegistry(props);
		Properties props2 = ParameterUtils.load(FileIntegratorRegistryTest.class.getClassLoader().getResourceAsStream("config/config_memoryStore.properties"));
		props2.setProperty(FileIntegratorRegistry.INTEGRATOR_REPOSITORY_URL, tempFolder.toURI().toURL().toString());
		fileRegistry = new FileIntegratorRegistry(props2);		
	}

	@Test 
	public void testDeleteMapping() throws DataSourceException, IntegratorRegistryException
	{
		IntegratedDataSourceMetadata iResource = fileRegistryRelative.retrieveIntegratedDataSourceMetadata("urn:ssg4e:iqs:CCO_SouthEastEngland_wave_IDS");
		logger.info("Source: "+  iResource.getSourceName());
		logger.info(iResource.getServiceDescription().getDocument());
		assertNotNull(iResource);
		assertNotNull(iResource.getServiceDescription().getDocument());		
	}
	
	@Test 
	public void testRetrieveIntegratedDataSourceMetadata() throws DataSourceException, IntegratorRegistryException
	{		
		IntegratedDataSourceMetadata iResource = 
			fileRegistryRelative.retrieveIntegratedDataSourceMetadata("urn:ssg4e:iqs:CCO_SouthEastEngland_wave_IDS");
		logger.info("Source: "+  iResource.getSourceName());
		logger.info(iResource.getServiceDescription().getDocument());
		assertNotNull(iResource);
		assertNotNull(iResource.getServiceDescription().getDocument());
		assertNotNull(iResource.getMapping());
		assertNotNull(iResource.getMapping().getUri());
		assertNotNull(iResource.getMapping().getLanguage());
	}
	
	@Test
	public void testCreateDeleteMapping() throws IntegratorRegistryException
	{		
		InputStream is = this.getClass().getClassLoader().getResourceAsStream("mappings/testMapping.r2r");
		MappingDocumentMetadata mappingDocument = new MappingDocumentMetadata("newMapping",MappingLanguage.R2RML,null);
		mappingDocument.setMapping(is);
		fileRegistry.storeMappingDocument(mappingDocument );
		fileRegistry.deleteMappingDocument(mappingDocument);
	}
	
	
	

}
