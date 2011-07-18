package es.upm.fi.dia.oeg.integration.algebra;

import java.util.Collection;
import java.util.Map;

import es.upm.fi.dia.oeg.integration.algebra.xpr.Xpr;

public interface OpInterface
{
	public String getId();
	public String getName();
	public OpInterface build(OpInterface op);
	//public String getExt();
	public void setId(String id);
	public void setName(String name);
	//public void setExt(String ext);
	public void display(int level);
	public void display();
	
	public OpInterface copyOp();
	public OpInterface merge(OpInterface op, Collection<Xpr> collection);
	
	public Map<String,Xpr> getVars();
}
