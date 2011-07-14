package es.upm.fi.dia.oeg.integration.metadata;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class DatasetMetadata
{
	private String uri;
	private GraphMetadata defaultGraph;
	
	private Map<String,NamedGraphMetadata> graphs;
	private Set<String> ontologies;
	
	public DatasetMetadata(String uri)
	{
		setUri(uri);
		defaultGraph = new GraphMetadata();
		graphs = Maps.newHashMap();
		ontologies = Sets.newHashSet();
	}
	
	public DatasetMetadata(String uri, Map<String,NamedGraphMetadata> namedGraphs)
	{
		this(uri);
		graphs.putAll(namedGraphs);
	}
	
	public Map<String,NamedGraphMetadata> getGraphs()
	{
		return graphs;
	}

	public void setDefaultGraph(GraphMetadata defaultGraph)
	{
		this.defaultGraph = defaultGraph;
	}

	public GraphMetadata getDefaultGraph()
	{
		return defaultGraph;
	}

	public void setUri(String uri)
	{
		this.uri = uri;
	}

	public String getUri()
	{
		return uri;
	}

	public void setOntologies(Set<String> ontologies)
	{
		this.ontologies = ontologies;
	}

	public Set<String> getOntologies()
	{
		return ontologies;
	}
}
