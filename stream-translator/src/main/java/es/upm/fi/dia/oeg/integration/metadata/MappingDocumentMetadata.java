package es.upm.fi.dia.oeg.integration.metadata;

import java.io.InputStream;
import java.net.URI;
import java.net.URL;

import org.w3c.dom.Document;

import es.upm.fi.dia.oeg.integration.metadata.mappings.MappingLanguage;
import es.upm.fi.dia.oeg.morph.r2rml.R2RModel;

public class MappingDocumentMetadata
{
	private String name;
	private InputStream mapping;
	private URI uri;
	private MappingLanguage language;
	
	public MappingDocumentMetadata(String name, MappingLanguage language, URI uri)
	{
		setName(name);
		setLanguage(language);
		setUri(uri);
	}
	
	public void setName(String mappingName)
	{
		this.name = mappingName;
	}
	public String getName()
	{
		return name;
	}
	public void setMapping(InputStream mapping)
	{
		this.mapping = mapping;
	}
	public InputStream getMapping()
	{
		return mapping;
	}
	public void setUri(URI uri)
	{
		this.uri = uri;
	}
	public URI getUri()
	{
		return uri;
	}
	public void setLanguage(MappingLanguage mappingLanguage) {
		this.language = mappingLanguage;
	}
	public MappingLanguage getLanguage() {
		return language;
	}

}
