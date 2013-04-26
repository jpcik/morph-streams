package es.upm.fi.oeg.sparqlstream;

import java.util.ArrayList;
import java.util.List;

import com.hp.hpl.jena.query.Query;

import es.upm.fi.oeg.sparqlstream.syntax.ElementStreamGraph;

public class StreamQuery extends Query 
{
	private List<ElementStreamGraph> streams;
	private boolean rstream=false;
	private boolean istream=false;
	private boolean dstream=false;
	
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

	public boolean isRstream() {
		return rstream;
	}

	public void setRstream(boolean rstream) {
		this.rstream = rstream;
	}

	public boolean isIstream() {
		return istream;
	}

	public void setIstream(boolean istream) {
		this.istream = istream;
	}

	public boolean isDstream() {
		return dstream;
	}

	public void setDstream(boolean dstream) {
		this.dstream = dstream;
	}
	
}
