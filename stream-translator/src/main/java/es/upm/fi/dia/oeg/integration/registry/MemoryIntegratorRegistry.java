package es.upm.fi.dia.oeg.integration.registry;

import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.Properties;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.log4j.Logger;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.xml.sax.SAXException;

import es.upm.fi.dia.oeg.integration.DataSourceException;
import es.upm.fi.dia.oeg.integration.metadata.IntegratedDataSourceMetadata;
import es.upm.fi.dia.oeg.integration.metadata.MappingDocumentMetadata;
import es.upm.fi.dia.oeg.integration.metadata.PullDataSourceMetadata;

public class MemoryIntegratorRegistry extends XMLIntegratorRegistry
{

	private Document integratorRepositoryDoc;
	private Document pullRepositoryDoc;
	//private Collection<IntegratedDataSourceMetadata> integratedResources;
	
	private static Logger logger = Logger.getLogger(MemoryIntegratorRegistry.class.getName());

	public MemoryIntegratorRegistry(Properties props) throws IntegratorRegistryException
	{
		super(props);
		integratorRepositoryDoc = super.loadIntegratedRespository();
		pullRepositoryDoc = super.loadPullRepository();
	}

	
	@Override
	public void storeMappingDocument(MappingDocumentMetadata mappingDocument)
			throws IntegratorRegistryException
	{
		
	}

	@Override
	public void removeAllPullDataResources() throws DataSourceException,
			IntegratorRegistryException
	{
		// TODO Auto-generated method stub
		
	}
	
	@Override
	protected Document loadIntegratedRespository() throws IntegratorRegistryException
	{
		return integratorRepositoryDoc;
	}

	@Override
	protected Document loadPullRepository() throws IntegratorRegistryException
	{
		return pullRepositoryDoc;
	}

	@Override
	protected void updatePullRepository(Document doc)
	{
		pullRepositoryDoc = doc;
		
	}
	
	@Override
	protected void updateIntegratedRepository(Document doc) throws IntegratorRegistryException
	{
		this.integratorRepositoryDoc = doc;
	}


	@Override
	public void deleteMappingDocument(MappingDocumentMetadata mappingDocument)
			throws IntegratorRegistryException {
		logger.info("Memory registry does not delete mappings");
	}



}
