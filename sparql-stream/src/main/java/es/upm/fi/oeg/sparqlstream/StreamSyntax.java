package es.upm.fi.oeg.sparqlstream;

import com.hp.hpl.jena.query.Syntax;

public class StreamSyntax extends Syntax 
{
	public static final Syntax syntaxSPARQLstream
        = new StreamSyntax("http://oeg-upm.net/query/SPARQLstream") ;
	   
	protected StreamSyntax(String s) 
	{
		super(s);
	}

	 static {
	        querySyntaxNames.put("sparqlstream",StreamSyntax.syntaxSPARQLstream) ;}
}
