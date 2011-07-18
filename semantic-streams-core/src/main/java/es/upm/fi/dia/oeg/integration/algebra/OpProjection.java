package es.upm.fi.dia.oeg.integration.algebra;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import com.google.common.collect.Maps;
import com.hp.hpl.jena.graph.Triple;


import es.upm.fi.dia.oeg.integration.algebra.xpr.BinaryXpr;
import es.upm.fi.dia.oeg.integration.algebra.xpr.OperationXpr;
import es.upm.fi.dia.oeg.integration.algebra.xpr.VarXpr;
import es.upm.fi.dia.oeg.integration.algebra.xpr.Xpr;

public class OpProjection extends OpUnary
{

	//public 
	private Map<String,Xpr> expressions;
	private Map<String,Set<String>> bindings;
	public Triple triple;
	public String link;
	
	private static Logger logger = Logger.getLogger(OpProjection.class.getName());

	public OpProjection(String id, OpInterface subOp)
	{
		super(id, "projection", subOp);
		expressions = new TreeMap<String,Xpr>();
		bindings = new HashMap<String,Set<String>>();
	}

	public boolean includes(OpProjection proj)
	{
		for (String exp:proj.expressions.keySet())
		{
			//if (!this.expressions.containsKey(exp))
			if (this.expressions.containsKey(exp))
			{
				if (!this.expressions.get(exp).isEqual(proj.getExpressions().get(exp)))
					return false;
				//VarXpr var1 =(VarXpr) (this.expressions.get(exp));  //TODO only vars taken into account!!!
				//VarXpr var2 =(VarXpr) (proj.expressions.get(exp));
				//if (!var1.getVarName().equals(var2.getVarName()))
					//return false;
			}
			else return false;
		}
		return true;
	}
	
	public Map<String,Set<String>> getBindings()
	{
		return bindings;
	}

	public void addBinding(String key,String value)
	{
		Set<String> bind = bindings.get(key); 
		if (bind==null)
		{
			bind = new HashSet<String>();
			bindings.put(key, bind);
		}
		bind.add(value);
	}
	
	public Map<String, Xpr> getExpressions()
	{
		return expressions;
	}

	public Map<String,Xpr> getVars()
	{
		Map<String,Xpr> vars = Maps.newHashMap();
		for (String k:expressions.keySet())
			vars.put(k, new VarXpr(k));
		return vars;
	}
	
	public void addExpression(String name,Xpr exp)
	{
		expressions.put(name, exp);
		
	}

	public Map<String,String> getVarMappings()
	{
		Map<String,String> varmap = Maps.newHashMap();
		for (Entry<String, Xpr> entry:expressions.entrySet())
		{
			if (entry.getValue() instanceof VarXpr)
			{
				VarXpr var = (VarXpr)entry.getValue();
				varmap.put(entry.getKey(),var.getVarName());
			}
		}
		return varmap;
	}
	
	@Override
	@Deprecated
	public String getString()
	{
		return getName()+" "+getId()+getExpressions().toString();
	}
	
	@Override
	public String toString()
	{		
		return getName()+" "+getId()+getExpressions().toString();
	}

	@Override
	public OpProjection copyOp()
	{
		OpProjection copy = new OpProjection(getId(), null);
		for (Map.Entry<String, Xpr> entry:expressions.entrySet())
		{
			copy.addExpression(entry.getKey(), entry.getValue());
			
		}
		if (getSubOp()!=null)
		{
			OpInterface op = getSubOp();
			copy.setSubOp(op.copyOp());
		}
		return copy;
	}

	@Override
	public OpInterface merge(OpInterface op,Collection<Xpr> xprs)
	{
		if (op==null) return this;
		if (op instanceof OpProjection)
			return this.merge((OpProjection)op,xprs);
		else if (op instanceof OpUnion)
		{
			OpUnion union = (OpUnion)op;
			 this.merge(union.getLeft(),xprs);
			this.merge(union.getRight(),xprs);
			OpUnion un = new OpUnion(merge(union.getLeft(),xprs),merge(union.getRight(),xprs));
			return un.simplify();
		}
		else 
		{
			for (Map.Entry<String, Xpr> entry: op.getVars().entrySet())
			{
				if (!this.getExpressions().containsKey(entry.getKey()))
				{
					this.getExpressions().put(entry.getKey(), entry.getValue());
				}
			}
		}
		return this;
	}


	
	private String removeColumn(String template)
	{
		//String res = template;
		while (template.indexOf('{')!=-1)
		{
			int i = template.indexOf('{');
			int f = template.indexOf('}');
			template = template.substring(0,i)+
					template.substring(f+1,template.length());					
		}
		return template;
	}
	
	public void add(OpProjection proj)
	{
		for (Map.Entry<String, Xpr> entry: proj.getExpressions().entrySet())
		{			
			if (!this.getExpressions().containsKey(entry.getKey()))
			{
				this.getExpressions().put(entry.getKey(), entry.getValue());
			}
		}
		if (this.getSubOp() == null)
		{
			this.setSubOp(proj.copyOp().getSubOp());
		}
	}
	
	public OpInterface merge(OpProjection proj,Collection<Xpr> xprs)
	{
		logger.debug("Merging projection: "+this +" and "+proj);
		for (Xpr xpr : xprs)
		{
			BinaryXpr bixpr = (BinaryXpr)xpr;
			VarXpr var = (VarXpr)bixpr.getLeft();
			logger.debug("expr: "+var+"--"+proj.getExpressions().get(var.getVarName()));
			if (this.getExpressions().get(var.getVarName())instanceof VarXpr)
			{
			VarXpr var1 = (VarXpr) this.getExpressions().get(var.getVarName());
			VarXpr var2 = (VarXpr) proj.getExpressions().get(var.getVarName());
			//logger.debug(var1);
			
			//logger.debug(var1+":"+removeColumn(var1.getModifier()));
			if (var1.getModifier()!=null && 
					!removeColumn(var1.getModifier()).equals(removeColumn(var2.getModifier())))
				return null;
			else if (this.getRelation().getExtentName().equals(proj.getRelation().getExtentName()))
			{				
				add(proj);
				return this;
			}
			}
			else if (expressions.get(var.getVarName()) instanceof OperationXpr)
			{
				OperationXpr opr1 = (OperationXpr)this.expressions.get(var.getVarName());
				OperationXpr opr2 = (OperationXpr)proj.expressions.get(var.getVarName());
				if (opr1.isEqual(opr2))
				{
					add(proj);
					return this;
				}
				else
					return null;
			}
		}
		//proj.getExpressions().
		/*
		if (proj.getRelation().getExtentName().equals(this.getRelation().getExtentName()))
		{
			for (Map.Entry<String, Xpr> entry: proj.getExpressions().entrySet())
			{
				
				if (!this.getExpressions().containsKey(entry.getKey()))
				{
					this.getExpressions().put(entry.getKey(), entry.getValue());
				}
			}
			if (this.getSubOp() == null)
			{
				this.setSubOp(proj.copyOp().getSubOp());
			}
		}
		else*/ if (proj.getRelation().getExtentName().equals("constants"))//TODO constants muct match, verify
		{
			for (Map.Entry<String, Xpr> entry: proj.getExpressions().entrySet())
			{
				if (this.getExpressions().containsKey(entry.getKey()))
				{
					if (!this.getExpressions().get(entry.getKey()).isEqual(entry.getValue()))
						return null;
				}
			}
		}
		
		else if (proj.getRelation().getExtentName().equals(""))//TODO fix empty relation
			return this;
		else //if (xprs!=null && xprs.size()>0)
		{
			OpJoin join = new OpJoin(this.copyOp(), proj.copyOp());
			/*
			join.getConditions().addAll(xprs);
			for (Xpr xpr:join.getConditions())
			{
				xpr.replaceVars(this.getVarMappings());
				xpr.replaceVars(proj.getVarMappings());
			}*/
			logger.debug("The new join ");
			join.display();
			return join;
		}
		//else return null;
		
		//logger.debug("Merged projection: "+this);
		return this;
	}
	
	@Override
	public OpInterface build(OpInterface newOp)
	{
		String type = newOp.getName();

		
		if (this.getId().equals("mainProjection"))//TODO this is hacky
		{
			if (getSubOp()==null)
				return newOp;
			else
			{
				OpInterface op = getSubOp().build(newOp);
				//this.setSubOp(op);
				return op;
			}
		}
				
		else if (type.equals("projection"))		
		{
			OpProjection projection = (OpProjection)newOp;
			OpJoin join = new OpJoin(null,this,projection);
			return join;
			
/*				
			if (this.getId().equals(projection.getId()))
			{
				OpUnion union = new OpUnion("unionid", this, newOp);
				union.setId(projection.getId());
				return union;
			}
			else return this;			
			*/
		}
		else if (newOp instanceof OpSelection)
		{ 
			OpSelection sel = (OpSelection)newOp;
			if (sel.getSubOp()==null) //only for empty selections!
			{
				sel.setSubOp(this.getSubOp());
				sel.replaceVars(this.getVarMappings());
				this.setSubOp(sel);
			}
			return this;
		
		}
		else if (type.equals("join"))
		{
			OpJoin join = (OpJoin)newOp;
				
			if (this.getId().equals(join.getLeft().getId()))
			{
				if (join.getLeft() instanceof OpProjection
							&& ((OpProjection)join.getLeft()).getExpressions().containsKey("placeholder"))
				{
					logger.info(join.getId());
					OpJoin joinCopy = join.copyOp();
					joinCopy.setLeft(this);
						
					return joinCopy;
				}
				else if (join.getLeft() instanceof OpProjection 
							&& this.includes((OpProjection)join.getLeft()))
				{
					OpJoin copy =join.copyOp();
					copy.setLeft(this);					
					return copy;
				}
				else
				{
					logger.info("what happened here: "+this.getString());
					logger.info("bap: "+join.getRight().getId());					
				}
			
			}
			else if (this.getId().equals(join.getRight().getId()))
			{
				if (join.getRight() instanceof OpProjection 
							&& this.includes((OpProjection)join.getRight()))
				{	
					OpJoin copy = join.copyOp();
					copy.setRight(this);
					return copy;
				}
				else				
				{	
					logger.info("what happened here2: "+this.getString());
					logger.info("bap2: "+join.getRight().getId());
				}
			}									
										
		}
		
		return this;
	}

	public void addVars(Map<String, Xpr> expressions)
	{
		for (Entry<String,Xpr> x:expressions.entrySet())
		{
			if (this.expressions.containsKey(x.getKey()))
			{
				Xpr exp =  this.expressions.get(x.getKey());
				if (exp instanceof VarXpr)
				{
					VarXpr var = (VarXpr)exp;
					//var.getAddVars().add(x.getValue().toString());
				}
			}
		}
		
	}
	
}
