package es.upm.fi.dia.oeg.sparqlstream;

import java.util.ArrayList;
import java.util.List;

import com.hp.hpl.jena.query.Query;

import es.upm.fi.dia.oeg.sparqlstream.syntax.ElementAggregate;
import es.upm.fi.dia.oeg.sparqlstream.syntax.ElementFunction;
import es.upm.fi.dia.oeg.sparqlstream.syntax.ElementStream;

public class StreamQuery extends Query 
{
	private List<ElementStream> streams;
	private List<ElementAggregate> aggregates;
	private List<ElementFunction> functions;
	
	
	public StreamQuery()
	{
		streams = new ArrayList<ElementStream>();
		aggregates = new ArrayList<ElementAggregate>();
		functions =new ArrayList<ElementFunction>() ;
	}
	
	public void setStreams(List<ElementStream> streams) {
		this.streams = streams;
	}
	public List<ElementStream> getStreams() {
		return streams;
	}
	public void setAggregates(List<ElementAggregate> aggregates) {
		this.aggregates = aggregates;
	}
	public List<ElementAggregate> getAggregates() {
		return aggregates;
	}
	
	public void addStream(ElementStream stream)
	{
		streams.add(stream);
	}
	
	public void addAggregate(ElementAggregate aggregate)
	{
		aggregates.add(aggregate);
	}

	public void addFunction(ElementFunction function)
	{
		functions.add(function);
	}

	public void setFunctions(List<ElementFunction> functions) {
		this.functions = functions;
	}

	public List<ElementFunction> getFunctions() {
		return functions;
	}

	public boolean containsStream(String uri) //TODO change to hash
	{
		return getStream(uri)!=null;
	}
	
	public ElementStream getStream(String uri)
	{
		for (ElementStream stream : this.streams)
		{
			if (stream.getUri().equals(uri)) return stream;
		}
		return null;
	}
	
	
}
