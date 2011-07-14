package es.upm.fi.dia.oeg.sparqlstream.syntax;

import com.hp.hpl.jena.sparql.syntax.Element;
import com.hp.hpl.jena.sparql.syntax.ElementVisitor;
import com.hp.hpl.jena.sparql.util.NodeIsomorphismMap;

import es.upm.fi.dia.oeg.common.TimeUnit;

public class TimeValue extends Element
{
	private long time;
	private WindowUnit unit;
	
	public TimeValue()
	{
		
	}
	
	public TimeValue(long time, WindowUnit timeUnit) {
		setTime(time);
		setUnit(timeUnit);
	}
	public long getTime() {
		return time;
	}
	public void setTime(long time) {
		this.time = time;
	}
	public WindowUnit getUnit() {
		return unit;
	}
	public void setUnit(WindowUnit unit) {
		this.unit = unit;
	}
	@Override
	public boolean equalTo(Element el2, NodeIsomorphismMap isoMap) {
		// TODO Auto-generated method stub
		return false;
	}
	@Override
	public int hashCode() {
		// TODO Auto-generated method stub
		return 0;
	}
	@Override
	public void visit(ElementVisitor v) {
		// TODO Auto-generated method stub
		
	}	

}
