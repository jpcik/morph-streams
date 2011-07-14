package es.upm.fi.dia.oeg.integration.registry;

import java.io.IOException;

public class IntegratorRegistryException extends Exception
{

	/**
	 * 
	 */
	private static final long serialVersionUID = -4785949461602681620L;
	
	public IntegratorRegistryException(String msg)
	{
		super(msg);
	}

	public IntegratorRegistryException(String message, Throwable e) {
		super(message,e);
	}

}

