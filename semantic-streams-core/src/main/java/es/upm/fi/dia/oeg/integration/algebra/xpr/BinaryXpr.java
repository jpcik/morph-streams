package es.upm.fi.dia.oeg.integration.algebra.xpr;

import java.util.Map;
import java.util.Set;

import com.google.common.collect.Sets;

public class BinaryXpr implements Xpr
{
	private Xpr left;
	private Xpr right;
	private String op;
	
	public static BinaryXpr createFilter(String varName, String op, String value)
	{
		BinaryXpr expr = new BinaryXpr();
		VarXpr var = new VarXpr(varName);
		ValueXpr val = new ValueXpr(value);
		expr.setLeft(var);
		expr.setRight(val);
		expr.setOp(op);
		return expr;
	}

	public static BinaryXpr createFilter(Xpr varLeft, String op, Xpr varRight)
	{
		BinaryXpr expr = new BinaryXpr();
		expr.setLeft(varLeft);
		expr.setRight(varRight);
		expr.setOp(op);
		return expr;
	}
	
	public Xpr getLeft()
	{
		return left;
	}
	public void setLeft(Xpr left)
	{
		this.left = left;
	}
	public Xpr getRight()
	{
		return right;
	}
	public void setRight(Xpr right)
	{
		this.right = right;
	}
	public String getOp()
	{
		return op;
	}
	public void setOp(String op)
	{
		this.op = op;
	}
	
	@Override
	public String toString()
	{
		return getLeft().toString() + " "+getOp() +" "+getRight().toString();
	}

	@Override
	public boolean isEqual(Xpr other)
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public Set<String> getVars()
	{
		return Sets.union(left.getVars(),right.getVars());
	}

	@Override
	public void replaceVars(Map<String, String> varNames)
	{
		left.replaceVars(varNames);
		right.replaceVars(varNames);
	}

	@Override
	public Xpr copy()
	{
		BinaryXpr copy = new BinaryXpr();
		copy.left = left.copy();
		copy.right = right.copy();
		copy.op = op;
		return copy;
	}

}
