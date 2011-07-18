package es.upm.fi.dia.oeg.integration.algebra;

import java.util.Collection;
import java.util.Map;

import org.apache.commons.lang.NotImplementedException;
import org.apache.log4j.Logger;

import com.hp.hpl.jena.sparql.algebra.Op;
import com.hp.hpl.jena.sparql.algebra.OpVisitor;
import com.hp.hpl.jena.sparql.algebra.Transform;
import com.hp.hpl.jena.sparql.algebra.op.Op1;
import com.hp.hpl.jena.sparql.util.NodeIsomorphismMap;

import es.upm.fi.dia.oeg.integration.algebra.xpr.Xpr;

public class OpUnary implements OpInterface
{
	protected String id;
	protected String name;
	//private String ext;

	protected OpInterface innerOp;
	private static Logger logger = Logger.getLogger(OpUnary.class.getName());

	public OpUnary(OpInterface subOp)
	{
		innerOp =  subOp;
	}

	public OpUnary(String id, String name, OpInterface subOp)
	{
		this(subOp);
		this.setId(id);
		this.setName(name);
	}

	public void setSubOp(OpInterface subOp)
	{
		innerOp = subOp;
	}

	public OpInterface getSubOp()
	{
		return innerOp;
	}
	

	

	@Override
	public String getName()
	{
		return name;
	}

	public void setId(String id)
	{
		this.id = id;
	}

	public String getId()
	{
		return id;
	}

	public void setName(String name)
	{
		this.name = name;
	}

	

	protected String tab(int level)
	{
		String res ="";
		for (int i=0;i<level;i++)
		{
			res += "\t";
		}
		return res;
	}
	
	public void display()
	{
		display(0);
	}
	
	public void display(int level)
	{
		logger.warn(tab(level)+getString());//+" ext:"+getExt());
		if (getSubOp()!= null)
		{
			((OpInterface)getSubOp()).display(level+1);
		}
				
	}
	public Map<String,Xpr> getVars()
	{
		return null;
	}
	
	public String getString()
	{
		return getName()+" "+getId();
	}
	
	public OpUnary(OpUnary unary)
	{
		this(unary.id, unary.name, unary.innerOp);
	}
/*
	@Override
	public String getExt()
	{
		return ext;
	}

	@Override
	public void setExt(String ext)
	{
		this.ext = ext;		
	}*/

	@Override
	public OpUnary copyOp()
	{
		OpUnary copy = new OpUnary(getId(),getName(),null);
		return copy;
	}

	@Override
	public OpInterface build(OpInterface op)
	{
		logger.error("Should never get here!");
		return null;
	}

	@Override
	public OpInterface merge(OpInterface op,Collection<Xpr> xprs)
	{
		throw new NotImplementedException("Merge operation not implemented for this operator "+this.name);

	}
	
	public OpRelation getRelation()
	{
		if (getSubOp()==null) 
			return null;
		else if (getSubOp()!=null && getSubOp() instanceof OpRelation)
			return (OpRelation)getSubOp();
		else
			return ((OpUnary)getSubOp()).getRelation();
		
	}
	
	
}
