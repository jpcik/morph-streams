package es.upm.fi.oeg.integration.adapter.esper;

import java.sql.ResultSet;
import java.util.List;
import java.util.Observable;

import org.w3.sparql.results.Sparql;

import com.espertech.esper.client.EPStatement;
import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.UpdateListener;
import com.google.common.collect.Lists;

import es.upm.fi.dia.oeg.integration.QueryException;
import es.upm.fi.dia.oeg.integration.QueryExecutor;
import es.upm.fi.dia.oeg.integration.Statement;

public class EsperStatement extends Observable implements UpdateListener,Statement
{
	EPStatement st;
	EsperQuery query;
	String spquery;
	
	public void addListener(EsperListener listener)
	{
		this.addObserver(listener);
		st.addListener(this);
	}

	public void update(EventBean[] newEvents, EventBean[] oldEvents) {
		//System.out.println("noi pasa nada");
		//System.out.println(newEvents[0].get("extentname"));
		EsperResultSet s = new EsperResultSet(newEvents, query);
		List<ResultSet> list = Lists.newArrayList();
		Sparql sp = null;
		list.add(s);
		try {
			sp = QueryExecutor.transfromData(list, query, spquery);
		} catch (QueryException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		setChanged();
		notifyObservers(sp);
		
	}
	
	
}
