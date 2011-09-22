package es.upm.fi.dia.oeg.integration;


import java.net.MalformedURLException;
import java.sql.ResultSet;
import java.util.List;
import java.util.Properties;

import es.upm.fi.dia.oeg.integration.metadata.SourceType;





public interface SourceAdapter 
{

	public abstract void addPullSource(String url, SourceType type) throws MalformedURLException, DataSourceException;

	public abstract void init(Properties props) throws StreamAdapterException;
	
	public abstract String invokeQueryFactory(String query, int duration) throws QueryCompilerException, QueryException;
	//public abstract String invokeQueryFactory(SourceQuery query, int duration) throws QueryCompilerException, QueryException;

	public abstract List<ResultSet> invokeQuery(String query, int duration) throws QueryCompilerException;
	public abstract List<ResultSet> invokeQuery(SourceQuery query) throws QueryCompilerException, QueryException;

	List<ResultSet> pullData(String queryId) throws QueryException;
	List<ResultSet> pullData(String queryId, int max) throws QueryException;

	List<ResultSet> pullNewestData(String queryId) throws QueryException;

	List<ResultSet> pullNewestData(String queryId, int max) throws QueryException;

	Statement registerQuery(SourceQuery query) throws QueryCompilerException, QueryException;
	
}
