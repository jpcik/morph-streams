package es.upm.fi.dia.oeg.integration;

public class InvalidResourceNameException extends DataSourceException
{


	/**
	 * 
	 */
	private static final long serialVersionUID = 7147137067845474226L;

	public InvalidResourceNameException(String msg)
	{
		super(msg);
	}

	public InvalidResourceNameException(String msg, Throwable e)
	{
		super(msg, e);
	}

}
