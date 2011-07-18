package es.upm.fi.dia.oeg.integration.metadata;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import com.google.common.collect.Lists;


public class IntegratedDataSourceMetadata extends DataSourceMetadata 
{
	/*
	private URI mappingUri;
	private String mappingName;
	private String mappingLanguage;*/
	private MappingDocumentMetadata mapping;
	private List<DataSourceMetadata> sourceList;
	private SPARQLServiceMetadata serviceDescription;
	
	public IntegratedDataSourceMetadata(String name, SourceType type, URI uri)
	{
		super(name,type,uri);
		sourceList = Lists.newArrayList();
	}
	/*
	public String getMappingName() {
		return mappingName;
	}
	public void setMappingName(String mapping) {
			this.mappingName = mapping;
	}*/
	public List<DataSourceMetadata> getSourceList() {
		return sourceList;
	}
	public void setSourceList(List<DataSourceMetadata> sourceList) {
		this.sourceList = sourceList;
	}/*
	public void setMappingUri(URI mappingUri) {
		this.mappingUri = mappingUri;
	}
	public void setMappingUri(String mappingUri) throws URISyntaxException 
	{
		this.mappingUri = new URI (mappingUri);
	}
	public URI getMappingUri() {
		return mappingUri;
	}*/

	public void setServiceDescription(SPARQLServiceMetadata serviceDescription)
	{
		this.serviceDescription = serviceDescription;
	}

	public SPARQLServiceMetadata getServiceDescription()
	{
		return serviceDescription;
	}
/*
	public void setMappingLanguage(String mappingLanguage)
	{
		this.mappingLanguage = mappingLanguage;
	}

	public String getMappingLanguage()
	{
		return mappingLanguage;
	}*/
	public void setMapping(MappingDocumentMetadata mapping) {
		this.mapping = mapping;
	}
	public MappingDocumentMetadata getMapping() {
		return mapping;
	}
	
	
}
