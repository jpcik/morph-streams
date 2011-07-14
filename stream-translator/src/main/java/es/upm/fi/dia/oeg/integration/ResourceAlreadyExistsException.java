package es.upm.fi.dia.oeg.integration;

public class ResourceAlreadyExistsException extends DataSourceException
{

	/**
	 * 
	 */
	private static final long serialVersionUID = 3863254113178087295L;

	public ResourceAlreadyExistsException(String msg)
	{
		super(msg);
	}

	public ResourceAlreadyExistsException(String msg, Throwable e)
	{
		super(msg, e);
	}

}
