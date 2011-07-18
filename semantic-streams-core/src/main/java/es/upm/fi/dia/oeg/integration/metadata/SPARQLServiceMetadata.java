package es.upm.fi.dia.oeg.integration.metadata;


import java.io.StringWriter;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Map;


import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.TransformerFactoryConfigurationError;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.apache.log4j.Logger;
import org.w3c.dom.Document;

import com.google.common.collect.Maps;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.vocabulary.RDF;

import es.upm.fi.dia.oeg.morph.r2rml.InvalidR2RDocumentException;
import es.upm.fi.dia.oeg.morph.r2rml.InvalidR2RLocationException;
import es.upm.fi.dia.oeg.morph.r2rml.R2RModel;
import es.upm.fi.dia.oeg.morph.r2rml.TriplesMap;
import es.upm.fi.dia.oeg.rdf.vocabulary.SPARQLServiceDescription;
import es.upm.fi.dia.oeg.rdf.vocabulary.Void;

import static es.upm.fi.dia.oeg.rdf.vocabulary.SPARQLServiceDescription.*;
import static es.upm.fi.dia.oeg.rdf.vocabulary.Void.*;

public class SPARQLServiceMetadata
{
	private static Logger logger = Logger.getLogger(SPARQLServiceMetadata.class.getName());
	//May use something else to represent the context, generate classes
	private String document;

	private String serviceUri;
	private String serviceUrl;
	private String supportedLanguage;
	private DatasetMetadata defaultDataset;
	
	private URI mappingUri;
	//private String integrationNamespace;
	
	public SPARQLServiceMetadata(URI uri) 
	{
		this.mappingUri = uri;
		//integrationNamespace = integrationNs;
	}
	
	public void setDocument(String document)
	{
		this.document = document;
	}

	public String getDocument()
	{
		if (document==null)
		{
			Model m = ModelFactory.createDefaultModel();
			
			m.setNsPrefix("sd", SPARQLServiceDescription.getUri() );
			m.setNsPrefix("void", Void.getUri());
			
			//m.setNsPrefix("scovo", "http://purl.org/NET/scovo#");
			if (getServiceUri()!=null)
			{
			Resource service = m.createResource(getServiceUri());
			
			if (getDefaultDataset()!=null)
			{
				Resource dataset = m.createResource(getDefaultDataset().getUri());
				
				for (NamedGraphMetadata gr:getDefaultDataset().getGraphs().values())
				{
					Resource streamGraph = m.createResource(gr.getUri());
					streamGraph.addLiteral(named, gr.getName());
					dataset.addProperty(namedGraph, streamGraph);
				}
				//Resource streamGraph = m.createResource(ssg4e+"CCOSouthEastEnglandWaveStream");
				//streamGraph.addLiteral(named, "http://www.semsorgrid4env/ccometeo.srdf");
				//dataset.addProperty(namedGraph, streamGraph);
				Resource graph = m.createResource();
				dataset.addProperty(defaultGraph, graph);
				
				for (String voc:getDefaultDataset().getOntologies())
					dataset.addLiteral(vocabulary, voc);
				//dataset.addLiteral(vocabulary, "http://www.semsorgrid4env.eu/ontologies/CoastalDefences.owl");
				//dataset.addLiteral(vocabulary, "http://www.semsorgrid4env.eu/ontologies/ObservationSource.owl");
				service.addProperty(SPARQLServiceDescription.defaultDataset, dataset);
				m.add(dataset,RDF.type,SPARQLServiceDescription.datasetType);
			}
		
			Resource sparqlStream = m.createResource(getSupportedLanguage());
			m.add(sparqlStream,RDF.type,language);
			service.addProperty(SPARQLServiceDescription.supportedLanguage,sparqlStream);
			if (getServiceUrl()!=null)
				service.addProperty(url,getServiceUrl());
			m.add(service, RDF.type, serviceType);
			}
			logger.debug("reading mapping: "+this.mappingUri);
			R2RModel r2r = new R2RModel();
			try
			{
				r2r.read(this.mappingUri);
			} catch (InvalidR2RDocumentException e)
			{
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InvalidR2RLocationException e)
			{
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			Resource dataset = m.createResource();
			Resource service = m.createResource(getServiceUri());
			service.addProperty(SPARQLServiceDescription.defaultDataset, dataset);
			m.add(dataset,RDF.type,SPARQLServiceDescription.datasetType);

			Map<String,Resource> graphs = Maps.newHashMap();
			
			for (TriplesMap tMap: r2r.getTriplesMap())
			{

				if (tMap.getSubjectMap().getGraphSet()!=null)
				{
					for (String graphName:tMap.getSubjectMap().getGraphSet())
					{
						
						Resource namedGraph = graphs.get(graphName);
						if (namedGraph==null) 
						{	namedGraph = m.createResource();
							namedGraph.addProperty(RDF.type, Void.datasetType);
							namedGraph.addProperty(RDF.type, SPARQLServiceDescription.graphType);
							namedGraph.addProperty(SPARQLServiceDescription.name, graphName);
							dataset.addProperty(SPARQLServiceDescription.namedGraph, namedGraph);
							graphs.put(graphName, namedGraph);
						}
						if (tMap.getSubjectMap().getRdfsClass()!=null)
						{
							Resource rdfclass = tMap.getSubjectMap().getRdfsClass();
							Resource classPart = m.createResource(graphName+"-"+rdfclass.getLocalName()); 
							namedGraph.addProperty(classPartition,classPart);
							classPart.addProperty(Void.classProperty, rdfclass);
						}
					}
				}
					
			}
			
			StringWriter sr = new StringWriter();
			m.write(sr);
			document = sr.toString();
			
		}
		return document;
	}

	private Model createRDF()
	{
		Model m = ModelFactory.createDefaultModel();
		
		String sd = "http://www.w3.org/ns/sparql-service-description#";
		m.setNsPrefix("sd", sd );
		Resource datasetType = m.createResource(sd+"Dataset");
		Resource serviceType = m.createResource(sd+"Service");
		Resource language = m.createResource(sd+"Language");
		Property supportedLanguage = m.createProperty(sd,"supportedLanguage");
		Property defaultDataset = m.createProperty(sd,"defaultDatasetDescription");
		Property defaultGraph = m.createProperty(sd,"defaultGraph");
		Property namedGraph = m.createProperty(sd,"namedGraph");
		Property url = m.createProperty(sd,"url");
		Property named = m.createProperty(sd,"named");
		
		String voidd = "http://rdfs.org/ns/void#";
		m.setNsPrefix("void", voidd);
		Property vocabulary = m.createProperty(voidd+"vocabulary");
		
		m.setNsPrefix("scovo", "http://purl.org/NET/scovo#");
		
		String ssg4e = "http://semsorgrid4env.eu/data/integration#";
		m.setNsPrefix("ssg", ssg4e);
		Resource service = m.createResource(ssg4e+"CCOSouthEastEnglandIntegratedService");
		Resource dataset = m.createResource(ssg4e+"CCOSouthEastEnglandWaveDataset");
		Resource streamGraph = m.createResource(ssg4e+"CCOSouthEastEnglandWaveStream");
		streamGraph.addLiteral(named, "http://www.semsorgrid4env/ccometeo.srdf");
		Resource graph = m.createResource();
		dataset.addProperty(defaultGraph, graph);
		dataset.addProperty(namedGraph, streamGraph);
		dataset.addLiteral(vocabulary, "http://www.semsorgrid4env.eu/ontologies/CoastalDefences.owl");
		dataset.addLiteral(vocabulary, "http://www.semsorgrid4env.eu/ontologies/ObservationSource.owl");
		Resource sparqlStream = m.createResource(ssg4e+"SPARQLStream");
		service.addProperty(supportedLanguage,sparqlStream);
		service.addProperty(url,"http://forgy.dia.fi.upm.es:8080/iqs-0.0.2/services/SPARQL");
		service.addProperty(defaultDataset, dataset);
		m.add(dataset,RDF.type,datasetType);
		m.add(service, RDF.type, serviceType);
		m.add(sparqlStream,RDF.type,language);
		
		return m;
	}
	
	public void load(Document doc) throws MetadataException
	{
				
			Transformer t;
			try
			{
				t = TransformerFactory.newInstance().newTransformer();
			} catch (TransformerConfigurationException e)
			{
				throw new MetadataException(
						"XML Transformer configuration error: " + e.getMessage(),e);
			} catch (TransformerFactoryConfigurationError e)
			{
				throw new MetadataException(
						"XML Transformer configuration error: " + e.getMessage(),e);
			}
			// t.setOutputProperty(OutputKeys.INDENT, "yes");
				StreamResult sr = new StreamResult(new StringWriter());
				try
				{
					t.transform(new DOMSource(doc),sr);
				} catch (TransformerException e)
				{
					throw new MetadataException(
							"XML Transformer configuration error: " + e.getMessage(),e);
				}
				setDocument(sr.getWriter().toString());
	}

	public void setServiceUri(String serviceUri)
	{
		this.serviceUri = serviceUri;
	}

	public String getServiceUri()
	{
		return serviceUri;
	}

	public void setServiceUrl(String string)
	{
		this.serviceUrl = string;
	}

	public String getServiceUrl()
	{
		return serviceUrl;
	}

	public void setSupportedLanguage(String supportedLanguage)
	{
		this.supportedLanguage = supportedLanguage;
	}

	public String getSupportedLanguage()
	{
		return supportedLanguage;
	}

	public void setDefaultDataset(DatasetMetadata defaultDataset)
	{
		this.defaultDataset = defaultDataset;
	}

	public DatasetMetadata getDefaultDataset()
	{
		return defaultDataset;
	}
}
