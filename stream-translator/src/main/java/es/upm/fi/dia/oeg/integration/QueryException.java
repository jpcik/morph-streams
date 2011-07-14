package es.upm.fi.dia.oeg.integration;

public class QueryException extends Exception
{
	/**
	 * 
	 */
	private static final long serialVersionUID = 2243488258416620324L;

	public QueryException(String msg)
	{
		super(msg);
	}
	
	public QueryException(Throwable e)
	{
		super(e);
	}
	
	public QueryException(String msg, Throwable e)
	{
		super(msg,e);
	}
}
