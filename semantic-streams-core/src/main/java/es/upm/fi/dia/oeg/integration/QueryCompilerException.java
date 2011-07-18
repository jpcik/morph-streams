package es.upm.fi.dia.oeg.integration;


public class QueryCompilerException extends QueryException
{

	/**
	 * 
	 */
	private static final long serialVersionUID = 8988811231961095425L;
	
	public QueryCompilerException(String msg)
	{
		super(msg);
	}
	
	public QueryCompilerException(Throwable e)
	{
		super(e);
	}

	public QueryCompilerException(String msg, Throwable e)
	{
		super(msg,e);
	}

}
