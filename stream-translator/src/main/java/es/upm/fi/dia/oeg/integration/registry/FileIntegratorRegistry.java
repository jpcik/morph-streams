package es.upm.fi.dia.oeg.integration.registry;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Properties;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.TransformerFactoryConfigurationError;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.apache.log4j.Logger;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import es.upm.fi.dia.oeg.integration.DataSourceException;
import es.upm.fi.dia.oeg.integration.InvalidResourceNameException;
import es.upm.fi.dia.oeg.integration.metadata.IntegratedDataSourceMetadata;
import es.upm.fi.dia.oeg.integration.metadata.MappingDocumentMetadata;
import es.upm.fi.dia.oeg.integration.metadata.MetadataException;
import es.upm.fi.dia.oeg.integration.metadata.mappings.MappingLanguage;
import es.upm.fi.dia.oeg.integration.metadata.mappings.R2RMLAdapter;
import es.upm.fi.dia.oeg.morph.r2rml.InvalidR2RDocumentException;

public class FileIntegratorRegistry extends XMLIntegratorRegistry
{

	private static Logger logger = Logger
			.getLogger(FileIntegratorRegistry.class.getName());


	public FileIntegratorRegistry(Properties props)
	{
		super(props);
	}


	@Override
	public void deleteMappingDocument(MappingDocumentMetadata mappingDocument) 
			throws IntegratorRegistryException
	{
		URI newUri = getMappingUri(mappingDocument);
		if (newUri.isAbsolute())			
		{
			File mappingFile = new File(newUri);
			if (mappingFile.exists())
			{
				logger.info("Mapping document exists. Will be deleted: "	+ newUri);
				boolean bl = mappingFile.delete();
				logger.info(bl);
			}
			else
				logger.info("Mapping document does not exist: " + newUri);
		}
		else
		{
			logger.info("Relative path mappings are never deleted.");
		}
	}


	@Override
	public void storeMappingDocument(MappingDocumentMetadata mappingDocument)
			throws IntegratorRegistryException
	{
		URI newUri = getMappingUri(mappingDocument);
		if (mappingDocument.getLanguage()==null)
			throw new IntegratorRegistryException("Mapping language not specified.");
		if (mappingDocument.getMapping()==null)
			throw new IntegratorRegistryException("Mapping body not specified.");
		
		File mappingFile = new File(newUri);
		if (mappingFile.exists())
			logger.info("Mapping document already exists. Will be overwriten: "	+ newUri);
		else
			logger.info("Mapping document will be created: " + newUri);

		//writeXml(mappingDocument.getMapping()., newUri);
		try
		{
			if (mappingDocument.getLanguage().equals(MappingLanguage.R2RML))
			{
				R2RMLAdapter adapter = new R2RMLAdapter();
				adapter.loadMapping(mappingDocument.getMapping());			
				adapter.write(newUri);
			}
			else
				throw new IntegratorRegistryException("Unable to process mapping language: "+mappingDocument.getLanguage());
		} catch (IOException e)
		{
			throw new IntegratorRegistryException(e.getMessage(),e);
		}
	}

	@Override
	public Collection<IntegratedDataSourceMetadata> getIntegratedDataResourceCollection()
			throws IntegratorRegistryException
	{
		Document doc = loadIntegratedRespository();

		Collection<IntegratedDataSourceMetadata> virtualSources = new ArrayList<IntegratedDataSourceMetadata>();
		IntegratedDataSourceMetadata integratedMD = null;
		NodeList sources = doc.getDocumentElement().getElementsByTagName(
				"virtualSource");
		for (int i = 0; i < sources.getLength(); i++)
		{
			Element e = (Element) sources.item(i);
			integratedMD = buildIntegratedDataSourceMetadata(e);
			virtualSources.add(integratedMD);
		}
		return virtualSources;

	}

	@Override
	public void removeAllPullDataResources() throws DataSourceException,
			IntegratorRegistryException
	{

		Document doc = loadPullRepository();

		Element sourceParent = (Element) doc.getDocumentElement()
				.getElementsByTagName("virtualSources").item(0);
		NodeList list = doc.getElementsByTagName("dataResource");
		for (int i = 0; i < list.getLength(); i++)
		{
			logger.info("Removing data resource");
			sourceParent.removeChild(list.item(i));
		}
		updatePullRepository(doc);

	}

}
