package es.upm.fi.dia.oeg.integration.algebra;


public class OpRoot extends OpUnary
{

	public OpRoot(OpInterface subOp)
	{
		super(subOp);
		setName("root");
	}

	
	@Override
	public OpInterface build(OpInterface newOp)
	{
		OpInterface op = null;
		if (this.getSubOp()==null)
			op = newOp;
		else					
			op =  this.getSubOp().build(newOp);
		
		this.setSubOp(op);
		return this;

	}
	


}
