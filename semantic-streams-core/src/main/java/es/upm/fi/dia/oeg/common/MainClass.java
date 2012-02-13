package es.upm.fi.dia.oeg.common;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;

import es.upm.fi.dia.oeg.sparqlstream.StreamQuery;
import es.upm.fi.dia.oeg.sparqlstream.StreamQueryFactory;

public class MainClass {
 public static void main(String args[]) throws MalformedURLException, IOException, URISyntaxException{
	 String queryfilename="file:///c:/query.sparql";
	 StreamQuery query = (StreamQuery) StreamQueryFactory.create( ParameterUtils.loadAsString(new URL(queryfilename)) );
	 query.getProjectVars();
 }
 
}
