package es.upm.fi.dia.oeg.integration.algebra.xpr;

import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

public class OperationXpr implements Xpr
{
	private Xpr param;
	private String op;
	private static Logger logger = Logger.getLogger(OperationXpr.class.getName());

	
	public OperationXpr(String operation, Xpr xpr)
	{
		setOp(operation);
		setParam(xpr);
	}
	public void setParam(Xpr param)
	{
		this.param = param;
	}
	public Xpr getParam()
	{
		return param;
	}
	public void setOp(String op)
	{
		this.op = op;
	}
	public String getOp()
	{
		return op;
	}
	
	@Override
	public String toString()
	{
		if (getOp().equals("constant"))
			return "'"+getParam()+"'";
		else
			return getOp()+"("+getParam()+")";
	}
	@Override
	public boolean isEqual(Xpr other)
	{
		//logger.debug("Comparing: "+this+" to "+other);
		if (this.op.equals("constant") && other instanceof ValueSetXpr)// FIXME: this is not an equality!
		{
			ValueSetXpr vsXpr = (ValueSetXpr)other;
			if (param instanceof ValueXpr)
			{
				ValueXpr val = (ValueXpr)param;
				return(vsXpr.getValueSet().contains(val.getValue()));
			}
		}
		if (!(other instanceof OperationXpr))
			return false;
		OperationXpr oper = (OperationXpr)other;
		boolean q1 = this.getOp().equals(oper.getOp());
		boolean q2 = this.getParam().isEqual(oper.getParam());
		return q1 && q2;
		//return this.getOp().equals(oper.getOp()) && this.getParam().isEqual(oper.getParam());
	}
	@Override
	public Set<String> getVars()
	{
		return param.getVars();
	}
	@Override
	public void replaceVars(Map<String, String> varNames)
	{
		param.replaceVars(varNames);
		
	}
	@Override
	public Xpr copy()
	{
		OperationXpr copy = new OperationXpr(op, param.copy());
		return copy;
	}
}
