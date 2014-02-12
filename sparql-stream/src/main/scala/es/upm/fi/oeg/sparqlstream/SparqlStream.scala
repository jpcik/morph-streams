package es.upm.fi.oeg.sparqlstream

import com.hp.hpl.jena.query.Syntax
import com.hp.hpl.jena.query.QueryFactory
import com.hp.hpl.jena.query.Query
import com.hp.hpl.jena.sparql.lang.SPARQLParserRegistry
import com.hp.hpl.jena.sparql.lang.SPARQLParserFactory
import com.hp.hpl.jena.sparql.lang.SPARQLParser
import es.upm.fi.oeg.sparqlstream.parser.ParserSPARQLstr

object SparqlStream {
  def parse(queryString:String)=
    SparqlStreamQueryFactory.create(queryString)//.asInstanceOf[StreamingQuery]
}

object SparqlStreamSyntax extends Syntax("http://oeg-upm.net/query/SPARQLstream"){
  Syntax.querySyntaxNames.put("sparqlstream",SparqlStreamSyntax)
}

object SparqlStreamQueryFactory extends QueryFactory{
  def create(queryString:String):StreamQuery=	{
	create(queryString,null,SparqlStreamSyntax)
  }
	
  def create(queryString:String, baseURI:String, querySyntax:Syntax)={
	val query = new StreamQuery
	if (!SPARQLParserRegistry.containsParserFactory(querySyntax)){
	  SPARQLParserRegistry.addFactory(SparqlStreamSyntax,                      
	    new SPARQLParserFactory(){
	      def accept(syntax:Syntax):Boolean=SparqlStreamSyntax.equals(syntax)   
	      def create(syntax:Syntax):SPARQLParser=new ParserSPARQLstr 
	    }) 
	}
	QueryFactory.parse(query, queryString, baseURI, querySyntax)
	query 
	       
  }	   
}
