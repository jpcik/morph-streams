package es.upm.fi.dia.oeg.integration.registry;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Properties;
import java.util.Set;

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

import com.google.common.collect.Sets;

import es.upm.fi.dia.oeg.integration.DataSourceException;
import es.upm.fi.dia.oeg.integration.InvalidResourceNameException;
import es.upm.fi.dia.oeg.integration.metadata.DataSourceMetadata;
import es.upm.fi.dia.oeg.integration.metadata.DatasetMetadata;
import es.upm.fi.dia.oeg.integration.metadata.IntegratedDataSourceMetadata;
import es.upm.fi.dia.oeg.integration.metadata.MappingDocumentMetadata;
import es.upm.fi.dia.oeg.integration.metadata.PullDataSourceMetadata;
import es.upm.fi.dia.oeg.integration.metadata.SPARQLServiceMetadata;
import es.upm.fi.dia.oeg.integration.metadata.SourceType;
import es.upm.fi.dia.oeg.integration.metadata.mappings.MappingLanguage;

public abstract class XMLIntegratorRegistry extends IntegratorRegistry
{
	protected static String INTEGRATOR_REPOSITORY_FILENAME = "mappings.xml";
	protected static String INTEGRATOR_PULL_REPOSITORY_FILENAME = "sources.xml";

	private static Logger logger = Logger.getLogger(XMLIntegratorRegistry.class.getName());

	public XMLIntegratorRegistry(Properties props)
	{
		registryProps = props;
	}
	
	@Override
	public IntegratedDataSourceMetadata retrieveIntegratedDataSourceMetadata(String sourceName) 
		throws DataSourceException, IntegratorRegistryException
	{
		IntegratedDataSourceMetadata intMD = null;

		Document doc = loadIntegratedRespository();
		NodeList sources = doc.getDocumentElement().getElementsByTagName("virtualSource");
		for (int i = 0; i < sources.getLength(); i++)
		{
			Element e = (Element) sources.item(i);
			String name = e.getAttribute("name");
			if (name.equals(sourceName))
			{
				intMD = buildIntegratedDataSourceMetadata(e);
				logger.info("Retrieving data resource: "+ intMD.getSourceName());
				break;
			}
		}

		return intMD;
	}

	protected URI getMappingUri(MappingDocumentMetadata mappingDocument) throws IntegratorRegistryException
	{
		if (mappingDocument.getName() == null)
		{
			String msg = "Invalid Mapping name: "+ mappingDocument.getName();
			logger.warn(msg);
			throw new IntegratorRegistryException(msg);
		}	
		
		URI newUri = null;
		String mappingUriString = getIntegratedRepositoryHome()
						+ mappingDocument.getName()+ "."
						+ getMappingExtension();
		logger.debug("Creating mapping: "+mappingUriString);
		try
		{
			newUri = new URI(mappingUriString);
		} catch (URISyntaxException e)
		{
			String msg = "Error generating mapping Uri for: "+ mappingDocument.getName();
			logger.warn(msg);
			throw new IntegratorRegistryException(msg);
		}
		return newUri;
	}
	


	@Override
	public void storeIntegratedDataSource(IntegratedDataSourceMetadata integratedMd)
			throws DataSourceException, IntegratorRegistryException
	{

		Document doc = loadIntegratedRespository();

		Element sourcesElement = (Element) doc.getDocumentElement()
				.getElementsByTagName("virtualSources").item(0);

		Element newSource = doc.createElement("virtualSource");
		newSource.setAttribute("name", integratedMd.getSourceName());
		newSource.setAttribute("uri", integratedMd.getUri().toString());
		String mappingName = integratedMd.getMapping().getName();
		newSource.setAttribute("mappingUri", mappingName+ "."+getMappingExtension());
		newSource.setAttribute("mappingName", integratedMd.getMapping().getName());
		if (integratedMd.getMapping().getLanguage()!=null)
			newSource.setAttribute("mappingLanguage", integratedMd.getMapping().getLanguage().toString());
		sourcesElement.appendChild(newSource);

		for (DataSourceMetadata ds : integratedMd.getSourceList())
		{
			Element orgSource = doc.createElement("dataSource");
			orgSource.setAttribute("name", ds.getSourceName());
			orgSource.setAttribute("location", ds.getUri().toString());
			orgSource.setAttribute("type", ds.getType().name());
			//orgSource.setAttribute("pullLocation", ds.getPullLocation());
			//orgSource.setAttribute("queryLocation", ds.getQueryLocation());
			newSource.appendChild(orgSource);
		}
		updateIntegratedRepository(doc);

	}

	

	
	@Override
	public Collection<IntegratedDataSourceMetadata> getIntegratedDataResourceCollection()
			throws IntegratorRegistryException
	{
		Document doc = loadIntegratedRespository();
		NodeList sources = doc.getDocumentElement().getElementsByTagName("virtualSource");
		Collection<IntegratedDataSourceMetadata> col = new ArrayList<IntegratedDataSourceMetadata>();
		IntegratedDataSourceMetadata intMD = null;
		for (int i = 0; i < sources.getLength(); i++)
		{
			Element e = (Element) sources.item(i);
			intMD = buildIntegratedDataSourceMetadata(e);

			logger.debug("Retrieving data resource: "+ intMD.getSourceName());
			col.add(intMD);
		}

		return col;
	}
	

	@Override
	public void removeIntegratedDataResource(String integratedSourceName)
			throws  IntegratorRegistryException, InvalidResourceNameException
	{

		Document pullDoc = loadPullRepository();
		Set<Integer> removeList = Sets.newHashSet();
		NodeList sources = pullDoc.getDocumentElement().getElementsByTagName("dataResource");
		for (int i = 0; i < sources.getLength(); i++)
		{
			Element e = (Element) sources.item(i);
			String name = e.getAttribute("virtualSourceName");
			if (name.equals(integratedSourceName))
			{
				removeList.add(i);
			}
		}
		for (Integer index:removeList)
		{
			Node sourceParent = pullDoc.getDocumentElement().getElementsByTagName("virtualSources").item(0);
			sourceParent.removeChild(sources.item(index));
			logger.info("Removing datasource, index: "+index);
		}
		updatePullRepository(pullDoc);
		
		
		Document doc = loadIntegratedRespository();
		int remove = -1;
		sources = doc.getDocumentElement().getElementsByTagName("virtualSource");
		for (int i = 0; i < sources.getLength(); i++)
		{
			Element e = (Element) sources.item(i);
			String name = e.getAttribute("name");
			if (name.equals(integratedSourceName))
			{
				remove = i;
			}
		}

		if (remove >= 0)
		{
			Node sourceParent = doc.getDocumentElement().getElementsByTagName("virtualSources").item(0);
			sourceParent.removeChild(sources.item(remove));
			logger.info("Removing datasource, index: "+remove);
		}
		else
		{
			String msg = "Datasource does not exist: "+integratedSourceName;
			logger.error(msg);
			throw new InvalidResourceNameException(msg);
		}
		updateIntegratedRepository(doc);
	}

	
	
	protected Document loadIntegratedRespository()
			throws IntegratorRegistryException
	{
		return getRepositoryDocument(INTEGRATOR_REPOSITORY_FILENAME);
	}
	
	protected Document loadPullRepository() throws IntegratorRegistryException
	{
		return getRepositoryDocument(INTEGRATOR_PULL_REPOSITORY_FILENAME);
	}
	
	protected  void updateIntegratedRepository(Document doc)
	throws IntegratorRegistryException
	{
		writeXml(doc, getIntegratedRepositoryURI());
	}



	private URI getIntegratedRepositoryURI() throws IntegratorRegistryException
	{
		URI uri = null;
		try
		{
			uri = new URI(getIntegratedRepositoryHome()	+ INTEGRATOR_REPOSITORY_FILENAME);
		} catch (URISyntaxException e)
		{
			String msg = "Invalid mapping repository Uri: ";
			logger.warn(msg);
			e.printStackTrace();
			throw new IntegratorRegistryException(msg);
		}
		return uri;
	}

	
	private Document getRepositoryDocument(String repositoryName)
			throws IntegratorRegistryException
	{

		URI repositoryUri = null;
		try
		{
			repositoryUri = new URI(getIntegratedRepositoryHome()
					+ repositoryName);
		} catch (URISyntaxException e1)
		{
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
		DocumentBuilder dBuilder = null;
		Document doc = null;

		try
		{
			dBuilder = dbf.newDocumentBuilder();
			if (repositoryUri.isAbsolute())
				doc = dBuilder.parse(new File(repositoryUri));
			else
			{
				InputStream is = XMLIntegratorRegistry.class.getClassLoader().getResourceAsStream(repositoryUri.toString());
				doc = dBuilder.parse(is);
			}
		} catch (SAXException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ParserConfigurationException e)
		{
			String msg = "Error parsing repository: " + repositoryName + ". "
					+ e.getMessage();
			e.printStackTrace();
			throw new IntegratorRegistryException(msg);
		} catch (Exception e)
		{
			String msg = "Error loading repository document: " + repositoryUri + ". "
			+ e.getMessage();
			e.printStackTrace();
			throw new IntegratorRegistryException(msg);
			
		}
		return doc;

	}

	protected IntegratedDataSourceMetadata buildIntegratedDataSourceMetadata(
			Element resourceElement) throws IntegratorRegistryException
	{
		String uri = resourceElement.getAttribute("uri");
		IntegratedDataSourceMetadata integratedMD = null;
		DataSourceMetadata sourceMD = null;

		MappingLanguage mappingLanguage = MappingLanguage.R2RML;//R2RML.getUri();
		if (resourceElement.hasAttribute("mappingLanguage"))
			mappingLanguage = MappingLanguage.valueOf(resourceElement.getAttribute("mappingLanguage"));
		URI mappingUri;
		//mappingdoc.setLanguage(mappingLanguage);
		//mappingdoc.setName(resourceElement.getAttribute("mappingName"));
		try
		{
			mappingUri = new URI(getIntegratedRepositoryHome()+resourceElement.getAttribute("mappingUri"));
		} catch (URISyntaxException e1)
		{
			String msg = "Invalid mapping uri: " + uri;
			throw new IntegratorRegistryException(msg,e1);
		}
		MappingDocumentMetadata mappingdoc = new MappingDocumentMetadata(resourceElement.getAttribute("mappingName"),mappingLanguage,mappingUri);

		URI intSourceUri = null; 
		try
		{
			intSourceUri = new URI(uri);
		} catch (URISyntaxException e1)
		{
			String msg = "Invalid integrated resource uri: " + uri;
			throw new IntegratorRegistryException(msg);
		}
		integratedMD = new IntegratedDataSourceMetadata(resourceElement.getAttribute("name"), SourceType.SERVICE, intSourceUri);
		integratedMD.setMapping(mappingdoc);
		SPARQLServiceMetadata description = null;
		
		description = new SPARQLServiceMetadata(integratedMD.getMapping().getUri());
		
		//S2OReader s2o = new S2OReader();
		//s2o.load(integratedMD.getMappingUri());
		description.setSupportedLanguage(getRegistryProps().getProperty(INTEGRATOR_VIRTUAL_URIBASE)+
				getRegistryProps().getProperty(INTEGRATOR_QUERY_DEFAULT_LANGUAGE));
		description.setServiceUri(integratedMD.getUri().toString());
		description.setServiceUrl("http://gato.com");
		
		DatasetMetadata dataset = new DatasetMetadata("uri:SampleDatasetUri");

		
		description.setDefaultDataset(dataset );
		//Document serviceDesc = getRepositoryDocument("service-descriptions/"+resourceElement.getAttribute("serviceDescription"));
		//description.load(serviceDesc);
		integratedMD.setServiceDescription(description);
		
		NodeList data = resourceElement.getElementsByTagName("dataSource");
		for (int j = 0; j < data.getLength(); j++)
		{
			Element ds = (Element) data.item(j);

			try {
				sourceMD = new DataSourceMetadata(ds.getAttribute("name"),null,
						new URI(ds.getAttribute("location")));
			} catch (URISyntaxException e) {
				throw new IntegratorRegistryException("Invalid Data source URI: "+ds.getAttribute("location"), e);
			}		
			if (ds.hasAttribute("type"))
				sourceMD.setType(SourceType.valueOf(ds.getAttribute("type")));
			else
			{
				if (sourceMD.getUri().toString().startsWith("udp"))
					sourceMD.setType(SourceType.UDP);
				else
					sourceMD.setType(SourceType.SERVICE);
			}
			//sourceMD.setQueryLocation(ds.getAttribute("queryLocation"));
			//sourceMD.setPullLocation(ds.getAttribute("pullLocation"));
			integratedMD.getSourceList().add(sourceMD);
		}
		return integratedMD;

	}
	/*
	private void loadMapping(IntegratedDataSourceMetadata im)
	{
		im.getMappingUri();
		
	}
	*/
	@Override
	public PullDataSourceMetadata retrievePullDataSourceMetadata(
			String sourceName) throws DataSourceException,
			IntegratorRegistryException
	{
		PullDataSourceMetadata intMD = null;

		Document doc = loadPullRepository();

		NodeList sources = doc.getDocumentElement().getElementsByTagName(
				"dataResource");
		for (int i = 0; i < sources.getLength(); i++)
		{
			Element e = (Element) sources.item(i);
			String name = e.getAttribute("name");
			if (name.equals(sourceName))
			{
				intMD = buildPullDataSourceMetadata(e);
				logger.debug("Retrieving data resource: " + sourceName);
			}
		}

		return intMD;
	}
	

	@Override
	public Collection<PullDataSourceMetadata> getPullDataResourceCollection()
			throws IntegratorRegistryException
	{
		Collection<PullDataSourceMetadata> res = new ArrayList<PullDataSourceMetadata>();
		PullDataSourceMetadata pullMD = null;
		Document doc = loadPullRepository();
		NodeList sources = doc.getDocumentElement().getElementsByTagName("dataResource");
		for (int i = 0; i < sources.getLength(); i++)
		{
			Element e = (Element) sources.item(i);
			//String name = e.getAttribute("name");
			pullMD = buildPullDataSourceMetadata(e);
			logger.debug("Retrieving data resource: " + pullMD.getSourceName());		
			res.add(pullMD);
		}

		return res;

	}
	

	@Override
	public void registerPullDataSource(PullDataSourceMetadata ds)
			throws IntegratorRegistryException
	{

		Document doc = loadPullRepository();

		Element queryElement = doc.createElement("query");
		queryElement.appendChild(doc.createTextNode(ds.getQuery()));
		queryElement.setAttribute("id", "" + ds.getQueryId());

		Element sourcesElement = (Element) doc.getDocumentElement()
				.getElementsByTagName("virtualSources").item(0);
		Element newSource = doc.createElement("dataResource");
		newSource.setAttribute("name", ds.getSourceName());
		newSource.setAttribute("uri", ds.getUri().toString());
		newSource.setAttribute("virtualSourceUri", ds.getVirtualSourceUri());
		newSource.setAttribute("virtualSourceName", ds.getVirtualSorceName());
		newSource.appendChild(queryElement);
		sourcesElement.appendChild(newSource);

		updatePullRepository(doc);

	}

	
	@Override
	public void removePullDataSource(String pullSourceName)
			throws IntegratorRegistryException, InvalidResourceNameException
	{

		Document doc = loadPullRepository();
		int remove = -1;
		NodeList sources = doc.getDocumentElement().getElementsByTagName("dataResource");
		for (int i = 0; i < sources.getLength(); i++)
		{
			Element e = (Element) sources.item(i);
			String name = e.getAttribute("name");
			if (name.equals(pullSourceName))
			{
				remove = i;
			}
		}

		if (remove >= 0)
		{
			Node sourceParent = doc.getDocumentElement().getElementsByTagName("virtualSources").item(0);
			sourceParent.removeChild(sources.item(remove));
		}
		else
		{
			throw new InvalidResourceNameException("Pull resource name not found: "+pullSourceName);
		}

		updatePullRepository(doc);

	}

	
	protected void updatePullRepository(Document doc)
			throws IntegratorRegistryException
	{
		writeXml(doc, getPullRepositoryURI());
	}


	private URI getPullRepositoryURI() throws IntegratorRegistryException
	{

		URI uri = null;
		try
		{
			uri = new URI(getIntegratedRepositoryHome()
					+ INTEGRATOR_PULL_REPOSITORY_FILENAME);
		} catch (URISyntaxException e)
		{
			String msg = "Invalid pull repository Uri: ";
			logger.warn(msg);
			e.printStackTrace();
			throw new IntegratorRegistryException(msg);
		}
		return uri;
	}

	
	protected void writeXml(Document doc, URI uri)
			throws IntegratorRegistryException
	{
		Transformer t;

		try
		{
			t = TransformerFactory.newInstance().newTransformer();
		} catch (TransformerConfigurationException e)
		{
			logger.warn(e.getMessage());
			e.printStackTrace();
			throw new IntegratorRegistryException(
					"XML Transformer configuration error: " + e.getMessage());
		} catch (TransformerFactoryConfigurationError e)
		{
			logger.warn(e.getMessage());
			e.printStackTrace();
			throw new IntegratorRegistryException(
					"XML Transformer configuration error: " + e.getMessage());
		}
		// t.setOutputProperty(OutputKeys.INDENT, "yes");
		try
		{
			t.transform(new DOMSource(doc), new StreamResult(new File(uri)));
		} catch (TransformerException e)
		{
			logger.warn(e.getMessage());
			e.printStackTrace();
			throw new IntegratorRegistryException("XML Transformer error: "
					+ e.getMessage());
		}

	}

	



	private PullDataSourceMetadata buildPullDataSourceMetadata(Element e) throws IntegratorRegistryException
	{
		PullDataSourceMetadata intMD = null;
		
		try
		{
			intMD = new PullDataSourceMetadata(e.getAttribute("name"), SourceType.SERVICE, new URI(e.getAttribute("uri")));			
		} catch (URISyntaxException e1)
		{
			throw new IntegratorRegistryException("Invalid source URI: "+e.getAttribute("uri"));
		}
		Element el = (Element) e.getFirstChild();
		intMD.setQuery(el.getTextContent());
		intMD.setQueryId((el.getAttribute("id")));
		intMD.setVirtualSourceUri(e.getAttribute("virtualSourceUri"));
		intMD.setVirtualSorceName(e.getAttribute("virtualSourceName"));
		return intMD;
	}

	

}
