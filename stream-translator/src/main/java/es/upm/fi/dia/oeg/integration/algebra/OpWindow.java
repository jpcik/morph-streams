package es.upm.fi.dia.oeg.integration.algebra;

import com.hp.hpl.jena.sparql.algebra.Op;


public class OpWindow extends OpRelation
{

	public OpWindow(String id, OpInterface subOp)
	{
		super(id);
		//setSubOp(subOp);
		setName("window");
	}
	
	private Window windowSpec;

	public void setWindowSpec(Window windowSpec)
	{
		this.windowSpec = windowSpec;
	}

	public Window getWindowSpec()
	{
		return windowSpec;
	}
	
	@Override
	public OpWindow copyOp()
	{
		//OpInterface copySubOp = getSubOp().copyOp();
		OpWindow copy = new OpWindow(id, null);
		copy.setExtentName(getExtentName());
		copy.setWindowSpec(getWindowSpec());
		return copy;
	}
	
	public String getString()
	{
		return getName()+" "+ getExtentName()+ " "+getWindowSpec();
	}

}
