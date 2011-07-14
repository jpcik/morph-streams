package es.upm.fi.dia.oeg.integration.translation;

import es.upm.fi.dia.oeg.integration.QueryCompilerException;

public class QueryTranslationException extends QueryCompilerException
{

	/**
	 * 
	 */
	private static final long serialVersionUID = 5644925373012716851L;

	public QueryTranslationException(String msg)
	{
		super(msg);
	}

	public QueryTranslationException(Throwable e)
	{
		super(e);
	}

	public QueryTranslationException(String msg, Throwable e)
	{
		super(msg, e);
	}

}
