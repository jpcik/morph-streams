package es.upm.fi.dia.oeg.r2o.stream;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.log4j.Logger;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import es.upm.fi.dia.oeg.integration.metadata.DatasetMetadata;
import es.upm.fi.dia.oeg.integration.metadata.NamedGraphMetadata;
import es.upm.fi.dia.oeg.integration.metadata.StreamNamedGraphMetadata;

public class S2OReader
{
	private static Logger logger = Logger.getLogger(S2OReader.class.getName());

	private DatasetMetadata defaultDataset;

	public DatasetMetadata getDefaultDataset()
	{
		return defaultDataset;
	}

	public void load(URI mappingUri) throws URISyntaxException,
			FileNotFoundException
	{
		if (logger.isDebugEnabled())
		{
			logger.debug("ENTER getInstance() with " + mappingUri);
		}
		File theMappingDoc = null;
		try
		{
			theMappingDoc = new File(mappingUri);
		} catch (IllegalArgumentException ex)
		{
			logger.debug(ex.getMessage());
			URL url = S2OReader.class.getClassLoader().getResource(mappingUri.toString());
			logger.info("The s2o file url: " + url);

			theMappingDoc = new File(url.toURI());
		}

		FileInputStream mfis = new FileInputStream(theMappingDoc);
		Document doc = null;

		DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
		factory.setValidating(false);
		DocumentBuilder docBuilder;
		try
		{
			docBuilder = factory.newDocumentBuilder();
			doc = docBuilder.parse(mfis);
		} catch (ParserConfigurationException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SAXException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		loadDataset(doc.getDocumentElement());

	}

	private void loadDataset(Element root)
	{
		Element datasetElement = (Element) root.getElementsByTagName("dataset")
				.item(0);
		Collection<NamedGraphMetadata> namedGraphs = new ArrayList<NamedGraphMetadata>();
		Collection<String> ontologies = new ArrayList<String>();
		NodeList streamGraphNodes = datasetElement
				.getElementsByTagName("namedStreamGraph");
		for (int i = 0; i < streamGraphNodes.getLength(); i++)
		{
			Element graphElement = (Element) streamGraphNodes.item(i);
			StreamNamedGraphMetadata graph = new StreamNamedGraphMetadata(
					graphElement.getAttribute("uri"),
					graphElement.getAttribute("name"));
			namedGraphs.add(graph);
		}
		NodeList vocabularyNodes = datasetElement
		.getElementsByTagName("vocabulary");
		for (int i = 0; i < vocabularyNodes.getLength(); i++)
		{
			Element vocElement = (Element) vocabularyNodes.item(i);
			ontologies.add(vocElement.getAttribute("ontologyUri"));
		}
		

	}
}
