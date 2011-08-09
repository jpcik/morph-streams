package es.upm.fi.dia.oeg.integration;

public class QueryDocument 
{
	private String queryString;

	public QueryDocument(String queryString)
	{
		setQueryString(queryString);
	}
	
	public String getQueryString() {
		return queryString;
	}

	public void setQueryString(String queryString) {
		this.queryString = queryString;
	}
	

}
