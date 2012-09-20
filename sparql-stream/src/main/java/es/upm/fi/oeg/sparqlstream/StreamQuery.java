package es.upm.fi.oeg.sparqlstream;

import java.util.ArrayList;
import java.util.List;

import com.hp.hpl.jena.query.Query;

import es.upm.fi.oeg.sparqlstream.syntax.ElementStreamGraph;

public class StreamQuery extends Query 
{
	private List<ElementStreamGraph> streams;
	
	public StreamQuery()
	{
		streams = new ArrayList<ElementStreamGraph>();
	}
	
	public void setStreams(List<ElementStreamGraph> streams) {
		this.streams = streams;
	}
	public List<ElementStreamGraph> getStreams() {
		return streams;
	}
	
	public void addStream(ElementStreamGraph stream)
	{
		streams.add(stream);
	}

	public boolean containsStream(String uri) //TODO change to hash
	{
		return getStream(uri)!=null;
	}
	
	public ElementStreamGraph getStream(String uri)
	{
		for (ElementStreamGraph stream : this.streams)
		{
			if (stream.getUri().equals(uri)) return stream;
		}
		return null;
	}
	
}
