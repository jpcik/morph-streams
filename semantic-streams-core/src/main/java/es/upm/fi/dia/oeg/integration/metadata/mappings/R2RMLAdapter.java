package es.upm.fi.dia.oeg.integration.metadata.mappings;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

import es.upm.fi.dia.oeg.integration.metadata.MetadataException;
import es.upm.fi.dia.oeg.morph.r2rml.InvalidR2RDocumentException;
import es.upm.fi.dia.oeg.morph.r2rml.R2RModel;

public class R2RMLAdapter 
{
	private R2RModel model;
	public void loadMapping(InputStream is) throws MetadataException  
	{		
		model = new R2RModel();
		try {
			model.read(is);
		} catch (InvalidR2RDocumentException e) {
			throw new MetadataException("Invalid R2RML mapping document",e);
		
		}		
	}
	
	public void write(URI uri) throws IOException
	{
		model.write(uri);
	}
}
