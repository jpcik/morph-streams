package es.upm.fi.dia.oeg.integration.algebra;

import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import com.google.common.collect.Sets;
import com.hp.hpl.jena.sparql.algebra.Op;

import es.upm.fi.dia.oeg.integration.algebra.xpr.Xpr;

public class OpSelection extends OpUnary
{
	private Xpr expression;
	
	private Set<Xpr> expressions;
	
	public OpSelection(String id, OpInterface subOp)
	{
		super(id, "selection", subOp);
		expressions = Sets.newHashSet();
	}

	public OpSelection(String id, OpInterface subOp, Set<Xpr> expressions)
	{
		super(id, "selection", subOp);
		this.expressions = Sets.newHashSet();
		for (Xpr xpr:expressions)
		{
			this.expressions.add(xpr.copy());
		}
	}

	public OpSelection(OpSelection selection)
	{
		this(selection.id,selection.innerOp);
	}

	@Deprecated
	public void setExpressiondep(Xpr expression)
	{
		this.expression = expression;
	}

	@Deprecated
	public Xpr getExpressiondep()
	{
		return expression;
	}
	
	public void addExpression(Xpr xpr)
	{
		expressions.add(xpr);
	}
	
	public Set<Xpr> getExpressions()
	{
		return expressions;
	}
	
	public Set<String> getSelectionVars()
	{
		Set<String> vars = Sets.newHashSet();
		for (Xpr xpr:expressions)
		{
			vars = Sets.union(vars, xpr.getVars());
		}
		return vars;
	}
	
	@Override
	public String getString()
	{
		return super.getString() +" ("+ getExpressions().toString()+")";
	}
	
	@Override
	public OpSelection copyOp()
	{
		OpSelection copy = new OpSelection(getId(), null);
		copy.expressions.addAll(expressions);
		if (getSubOp() != null)
		{
			OpInterface op = (OpInterface)getSubOp();
			copy.setSubOp(op.copyOp());
		}
		return copy;
	}
	
	@Override
	public OpInterface build(OpInterface newOp)
	{
		//throw new IllegalAccessError();
		setSubOp(getSubOp().build(newOp));
		
		return this;
	}

	public void replaceVars(Map<String, String> varNames)
	{
		for (Xpr xpr:this.expressions)
		{
			xpr.replaceVars(varNames);
				
		}
		
	}

	
}
