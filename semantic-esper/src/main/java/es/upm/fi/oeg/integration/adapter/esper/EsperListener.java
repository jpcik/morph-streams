package es.upm.fi.oeg.integration.adapter.esper;

import java.util.Observable;
import java.util.Observer;

import org.w3.sparql.results.Sparql;

import com.hp.hpl.jena.graph.Triple;

import es.upm.fi.dia.oeg.common.Utils;

public class EsperListener implements Observer
{
	  public  void update(Triple t){
	  
	  }

	public void update(Observable o, Object arg) 
	{
		Utils.printSparqlResult((Sparql)arg);
		System.out.println(arg);
		//System.out.println("papas: "+arg);
		
	}
	  
}
