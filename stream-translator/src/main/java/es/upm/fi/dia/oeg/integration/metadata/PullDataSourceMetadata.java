package es.upm.fi.dia.oeg.integration.metadata;

import java.net.URI;


public class PullDataSourceMetadata extends DataSourceMetadata 
{
	private String query;
	private String queryId;
	private String virtualSourceUri;
	private String virtualSorceName;
	
	public PullDataSourceMetadata(String name, SourceType type, URI uri)
	{
		super(name,type,uri);
	}
	
	public String getQueryId() {
		return queryId;
	}

	public void setQueryId(String queryId) {
		this.queryId = queryId;
	}

	public void setQuery(String query) {
		this.query = query;
	}

	public String getQuery() {
		return query;
	}

	public void setVirtualSourceUri(String virtualSourceUri) {
		this.virtualSourceUri = virtualSourceUri;
	}

	public String getVirtualSourceUri() {
		return virtualSourceUri;
	}

	public void setVirtualSorceName(String virtualSorceName)
	{
		this.virtualSorceName = virtualSorceName;
	}

	public String getVirtualSorceName()
	{
		return virtualSorceName;
	}
}
