package es.upm.fi.dia.oeg.integration.registry;

import java.net.URISyntaxException;
import java.util.Collection;
import java.util.Properties;


import es.upm.fi.dia.oeg.integration.DataSourceException;
import es.upm.fi.dia.oeg.integration.InvalidResourceNameException;
import es.upm.fi.dia.oeg.integration.metadata.IntegratedDataSourceMetadata;
import es.upm.fi.dia.oeg.integration.metadata.MappingDocumentMetadata;
import es.upm.fi.dia.oeg.integration.metadata.PullDataSourceMetadata;

public abstract class IntegratorRegistry 
{
	public static String INTEGRATOR_VIRTUAL_URIBASE = "integrator.data.uribase";
	public static String INTEGRATOR_QUERY_DEFAULT_LANGUAGE = "integrator.queryexecutor.defaultlanguage";
	public static String INTEGRATOR_REPOSITORY_URL = "integrator.repository.url";
	public static String INTEGRATOR_MAPPING_EXTENSION = "integrator.mapping.extension";
	public static String INTEGRATOR_MAPPING_LANGUAGE = "integrator.mapping.language";
	protected Properties registryProps;

	public Properties getRegistryProps() {
		return registryProps;
	}
	public void setRegistryProps(Properties registryProps) {
		this.registryProps = registryProps;
	}
	
	protected String getMappingExtension()
	{
		Object ext = registryProps.get(INTEGRATOR_MAPPING_EXTENSION);
		return ext==null?"ttl":ext.toString();
	}
	public abstract void storeMappingDocument(MappingDocumentMetadata mappingDocument) 
			throws IntegratorRegistryException;
	public abstract void deleteMappingDocument(MappingDocumentMetadata mappingDocument) 
		throws IntegratorRegistryException;
	
	public abstract void storeIntegratedDataSource(IntegratedDataSourceMetadata integratedMd) 
			throws DataSourceException, IntegratorRegistryException;
	public abstract IntegratedDataSourceMetadata retrieveIntegratedDataSourceMetadata(String sourceName) 
			throws DataSourceException, IntegratorRegistryException;
	public abstract Collection<IntegratedDataSourceMetadata> getIntegratedDataResourceCollection() 
			throws IntegratorRegistryException;
	public abstract void removeIntegratedDataResource(String integratedSourceName) 
			throws InvalidResourceNameException, IntegratorRegistryException;
	
	public abstract void registerPullDataSource(PullDataSourceMetadata ds) 
			throws IntegratorRegistryException;
	public abstract PullDataSourceMetadata retrievePullDataSourceMetadata(String sourceName) 
			throws DataSourceException, IntegratorRegistryException;
	public abstract Collection<PullDataSourceMetadata> getPullDataResourceCollection() 
			throws IntegratorRegistryException;
	public abstract void removePullDataSource(String pullSourceName) 
			throws InvalidResourceNameException, IntegratorRegistryException ;
	public abstract void removeAllPullDataResources() 
		throws DataSourceException, IntegratorRegistryException ;
	
	protected String getIntegratedRepositoryHome()
	{
		
		String repo = this.getRegistryProps().getProperty(INTEGRATOR_REPOSITORY_URL);
	
		if (repo.equals("tmp"))
		{
			return "file:///"+System.getProperty("user.dir").replace('\\', '/')+"/src/test/resources/mappings/";
		}
		
		return repo;
	}
	



}
