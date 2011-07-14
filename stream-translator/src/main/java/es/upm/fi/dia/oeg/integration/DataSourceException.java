package es.upm.fi.dia.oeg.integration;

public class DataSourceException extends Exception 
{
	public DataSourceException(String msg)
	{
		super(msg);
	}

	public DataSourceException(String msg, Throwable e)
	{
		super(msg,e);
	}

	/**
	 * serialization uid
	 */
	private static final long serialVersionUID = 7967321780611657249L;
}
