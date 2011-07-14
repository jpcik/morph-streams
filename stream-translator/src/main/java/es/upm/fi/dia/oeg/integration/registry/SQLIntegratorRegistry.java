package es.upm.fi.dia.oeg.integration.registry;

import java.net.URI;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Properties;

import org.apache.log4j.Logger;

import es.upm.fi.dia.oeg.integration.DataSourceException;
import es.upm.fi.dia.oeg.integration.metadata.IntegratedDataSourceMetadata;
import es.upm.fi.dia.oeg.integration.metadata.MappingDocumentMetadata;
import es.upm.fi.dia.oeg.integration.metadata.PullDataSourceMetadata;


public class SQLIntegratorRegistry extends IntegratorRegistry
{

	private static Logger logger = Logger.getLogger(SQLIntegratorRegistry.class.getName());
	
	public SQLIntegratorRegistry(Properties props)
	{
		registryProps = props;		
	}
	
	private Connection getConnection()
	{
		try {
			Class.forName("com.mysql.jdbc.Driver").newInstance();
		} catch (InstantiationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}  		
  	    
		Connection conn = null;
		try {
			conn = DriverManager.getConnection(registryProps.getProperty("integrator.repository.url"),
					registryProps.getProperty("integrator.repository.user"),
					registryProps.getProperty("integrator.repository.password"));
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return conn;
	}
	
	public void storeIntegratedDataSource(IntegratedDataSourceMetadata integratedDS) throws DataSourceException
	{
		 Connection conn = null;				
         PreparedStatement pStmt = null;
         
         conn = getConnection();
         try {
			pStmt = conn.prepareStatement
		      ("INSERT INTO dataresource (name,uri,virtual,mappingUri) values (?,?,?,?)");
			pStmt.setString(1, integratedDS.getSourceName());
			pStmt.setString(2, integratedDS.getUri().toString());
			pStmt.setBoolean(3, true);
			pStmt.setString(4, integratedDS.getMapping().getName().toString());
			pStmt.executeUpdate();
			logger.info("Added integrated data source: "+integratedDS.getUri().toString());
			conn.close();
			
		} catch (SQLException e) 
		{
			String msg = "Error storing the data resource.";
			logger.error(msg);					
			e.printStackTrace();
			throw new DataSourceException(msg);
		}
                   		
	}
	
	public void removeIntegratedDataResource(String dataResourceName) throws IntegratorRegistryException 
	{
		Connection conn = null;
        PreparedStatement pStmt = null;
        
        conn = getConnection();
        try {
			pStmt = conn.prepareStatement
			      ("DELETE FROM dataresource WHERE uri = ?");
			pStmt.setString(1, dataResourceName);
			pStmt.executeUpdate();
			logger.info("Removed data resource: "+dataResourceName);
			conn.close();
		} catch (SQLException e) 
		{
			String msg = "Error removing the data resource.";
			logger.error(msg);					
			e.printStackTrace();
			throw new IntegratorRegistryException(msg);
		}
		
		
	}

	public IntegratedDataSourceMetadata retrieveIntegratedDataSourceMetadata(String sourceName) throws DataSourceException
	{
		IntegratedDataSourceMetadata intMD = null;
		
		Connection conn = null;
		
        PreparedStatement pStmt = null;
        ResultSet rs = null;
        
        conn = getConnection();
        try {
			pStmt = conn.prepareStatement
			      ("SELECT mappingUri, name FROM dataresource WHERE uri = ?");
			pStmt.setString(1, sourceName);
			rs = pStmt.executeQuery();
			rs.first();
			//intMD.set.setMappingUri(rs.getString("mappingUri"));
			//intMD = new IntegratedDataSourceMetadata(rs.getString("name"), type, location)
			//intMD.setSourceName();
			//intMD.setUri(new URI (rs.getString("uri")));
			logger.info("Retrieving data resource: "+sourceName);
			conn.close();
		} catch (SQLException e) 
		{
			String msg = "Error removing the data resource.";
			logger.error(msg);					
			e.printStackTrace();
			throw new DataSourceException(msg);
		}
		
		
		return intMD;
	}

	@Override
	public void storeMappingDocument(MappingDocumentMetadata mapping) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Collection<IntegratedDataSourceMetadata> getIntegratedDataResourceCollection()
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Collection<PullDataSourceMetadata> getPullDataResourceCollection()
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void registerPullDataSource(PullDataSourceMetadata ds)
	{
		// TODO Auto-generated method stub
		
	}

	@Override
	public void removePullDataSource(String pullSourceString)
	{
		// TODO Auto-generated method stub
		
	}

	@Override
	public PullDataSourceMetadata retrievePullDataSourceMetadata(String sourceString)
			throws DataSourceException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void removeAllPullDataResources() throws DataSourceException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void deleteMappingDocument(MappingDocumentMetadata mappingDocument)
			throws IntegratorRegistryException {
		// TODO Auto-generated method stub
		
	}
}
