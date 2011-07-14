package es.upm.fi.dia.oeg.integration.metadata;

public class NamedGraphMetadata extends GraphMetadata
{
	private String name;
	private String uri;

	public NamedGraphMetadata(String uri, String name)
	{
		setUri(uri);
		setName(name);
	}
	
	public void setName(String name)
	{
		this.name = name;
	}

	public String getName()
	{
		return name;
	}

	public void setUri(String uri)
	{
		this.uri = uri;
	}

	public String getUri()
	{
		return uri;
	}

}
