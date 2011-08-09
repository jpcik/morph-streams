package es.upm.fi.dia.oeg.integration.algebra;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.ListUtils;
import org.apache.commons.lang.NotImplementedException;
import org.apache.log4j.Logger;

import com.google.common.collect.Collections2;
import com.google.common.collect.Maps;
import com.hp.hpl.jena.sparql.algebra.Op;
import com.hp.hpl.jena.sparql.algebra.OpVisitor;
import com.hp.hpl.jena.sparql.algebra.Transform;
import com.hp.hpl.jena.sparql.algebra.op.Op2;
import com.hp.hpl.jena.sparql.util.NodeIsomorphismMap;

import es.upm.fi.dia.oeg.integration.algebra.xpr.ValueSetXpr;
import es.upm.fi.dia.oeg.integration.algebra.xpr.Xpr;

public class OpBinary implements OpInterface
{

	private OpInterface left;
	private OpInterface right;
	private String id;
	private String name;
	private static Logger logger = Logger.getLogger(OpBinary.class.getName());

	public OpBinary(String id, String name, OpInterface left, OpInterface right)
	{
		this(left, right);
		this.setId(id);
		this.setName(name);
	}
	
	public OpBinary(OpInterface left, OpInterface right)
	{
		this.left = left;
		this.right = right;
	}


	@Override
	public String getName()
	{
		// TODO Auto-generated method stub
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
	
	
	public OpInterface getLeft()
	{
		return this.left;
	}
	public OpInterface getRight()
	{
		return this.right;
	}

	public void setLeft(OpInterface left)
	{
		this.left = left;
	}
	
	public void setRight(OpInterface right)
	{
		this.right = right;
	}
	
	private String tab(int level)
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

	public String toString()
	{
		return getName()+" "+getId();
	}
	
	public Map<String,Xpr> getVarsNull(OpInterface op)
	{
		if (op==null)
			return Maps.newHashMap();
		else
			return op.getVars();
	}
	
	@SuppressWarnings("unchecked")
	public Map<String,Xpr> getVars()
	{
		logger.debug("vars: "+this.left+this.right);
		//this.display();
		HashMap<String,Xpr> map = new HashMap<String, Xpr>();
		Map<String,Xpr> leftvars= getVarsNull(left);
		Map<String,Xpr> rightvars= getVarsNull(right);
		Collection<String> left = CollectionUtils.subtract(leftvars.keySet(), rightvars.keySet());
		Collection<String> right = CollectionUtils.subtract(rightvars.keySet(), leftvars.keySet());
		Collection<String> inter = CollectionUtils.intersection(leftvars.keySet(), rightvars.keySet());
		for (String k:left)
			map.put(k, leftvars.get(k));
		for (String k:right)
			map.put(k, rightvars.get(k));
		for (String k:inter)
		{
			Xpr lxpr=leftvars.get(k);
			Xpr rxpr=rightvars.get(k);
			if (lxpr.isEqual(rxpr))
				map.put(k, lxpr);
			else
			{
				ValueSetXpr vs = new ValueSetXpr();
				vs.getValueSet().add(lxpr.toString());
				vs.getValueSet().add(rxpr.toString());
				map.put(k, vs);
			}
			
		}
		
		//if (getRight()!=null)
		//	map.putAll(this.getRight().getVars());
		logger.debug("Retunrn vars: "+map);
		return map;
	}
	
	public void display(int level)
	{
		logger.warn(tab(level)+this.toString());
		if (getLeft()!= null)
		{
			getLeft().display(level+1);
			/*
			if (getLeft() instanceof OpUnary)
			{
				((OpUnary)getLeft()).display(level+1);
			}
			else
			{
				((OpBinary)getLeft()).display(level+1);
			}*/
		}
		if (getRight()!= null)
		{
			getRight().display(level+1);
			/*
			if (getRight() instanceof OpUnary)
			{
				((OpUnary)getRight()).display(level+1);
			}
			else
			{
				((OpBinary)getRight()).display(level+1);
			}*/
		}
		
	}

	public OpInterface simplify()
	{
		if (this.getLeft()==null)
			return this.getRight();
		else if (this.getRight()==null)
			return this.getLeft();
		return this;
	}
	

	
	@Override
	public OpBinary  copyOp()
	{
		OpBinary copy = new OpBinary(getId(), getName(), null, null);
		return copy;
	}

	@Override
	public OpInterface build(OpInterface op)
	{
		logger.error("Should never get here!"+this.getName());
		return null;
	}

	@Override
	public OpInterface merge(OpInterface op, Collection<Xpr> xprs)
	{
		throw new NotImplementedException("No implementation for OpBinary "+this.name);
	}


}
