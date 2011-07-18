package es.upm.fi.dia.oeg.integration.metadata;

import java.net.URI;

public class DataSourceMetadata 
{
	private String sourceName;
	private URI uri;
	//private String location;
	private String user;
	private String password;
	private SourceType type;
	
	public DataSourceMetadata(String name, SourceType type, URI uri)
	{
		setSourceName(name);
		setType(type);
		setUri(uri);
	}
	
	//private String pullLocation;
	/*
	public void setPullLocation(String pullLocation)
	{
		this.pullLocation = pullLocation;
	}
	public void setQueryLocation(String queryLocation)
	{
		this.queryLocation = queryLocation;
	}
	private String queryLocation;

	public String getLocation() {
		return location;
	}
	public void setLocation(String location) {
		this.location = location;
	}*/
	public String getUser() {
		return user;
	}
	public void setUser(String user) {
		this.user = user;
	}
	public String getPassword() {
		return password;
	}
	public void setPassword(String password) {
		this.password = password;
	}
	public SourceType getType() {
		return type;
	}
	public void setType(SourceType type) {
		this.type = type;
	}
	public String getSourceName() {
		return sourceName;
	}
	public void setSourceName(String sourceName) {
		this.sourceName = sourceName;
	}
	public URI getUri() {
		return uri;
	}
	public void setUri(URI uri) {
		this.uri = uri;
	}/*
	public String getPullLocation()
	{
		return pullLocation;
	}
	public String getQueryLocation()
	{
		return queryLocation;
	}
	*/
	

}
