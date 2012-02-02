package es.upm.fi.dia.oeg.integration.algebra;

import java.util.ArrayList;
import java.util.Collection;


import org.apache.commons.lang.NotImplementedException;
import org.apache.log4j.Logger;

import com.google.common.collect.Lists;


import es.upm.fi.dia.oeg.integration.algebra.xpr.BinaryXpr;
import es.upm.fi.dia.oeg.integration.algebra.xpr.VarXpr;
import es.upm.fi.dia.oeg.integration.algebra.xpr.Xpr;

public class OpJoin extends OpBinary
{
	
	private Collection<Xpr> conditions;
	private static Logger logger = Logger.getLogger(OpJoin.class.getName());
	private static final String OP_JOIN = "join";
	
	public OpJoin(OpInterface left, OpInterface right)
	{
		this(null,left,right,true);
	}
	public OpJoin(String id, OpInterface left, OpInterface right)
	{
		this(id,left,right,true);
	}

	public OpJoin(String id, OpInterface left, OpInterface right, boolean generateConditions)
	{				
		super(id, OP_JOIN,left, right);
		conditions = new ArrayList<Xpr>();
		if (generateConditions)
		//if (left instanceof OpProjection && right instanceof OpProjection)
		{
			//OpProjection leftPro = (OpProjection)left;
			//OpProjection rightPro = (OpProjection)right;
			if (left!=null)
			for (String key:left.getVars().keySet())
			{
				
				if (right!=null && right.getVars()!=null && right.getVars().containsKey(key))
				{
					BinaryXpr e = new BinaryXpr();
					e.setLeft(left.getVars().get(key));
					e.setRight(right.getVars().get(key));
					e.setOp("=");
					this.addCondition(e);					
				}
				else
				{
					this.toString();
				}
			}
		}

	}
	
	public void addCondition(Xpr e)
	{
		conditions.add(e);
	}
	
	public Collection<Xpr> getConditions()
	{
		return conditions;
	}
	
	
	
	public String toString()
	{
		String cond = "";
		for (Xpr x:conditions)
		{
			cond+=x.toString()+" ";
		}
		return super.toString()+" ("+cond+")";
	}
	
	public boolean hasEqualConditions()
	{
		if (conditions.isEmpty()) return false;
		for (Xpr x:conditions)
		{
			if (x instanceof BinaryXpr)
			{
				BinaryXpr bXpr = (BinaryXpr)x;
				if (!bXpr.getOp().equals("=") || !bXpr.getLeft().isEqual(bXpr.getRight()))
					return false;
			}
		}
		return true;
	}
	
	public boolean includes(OpJoin join)
	{
		return 
		join.getLeft() instanceof OpProjection && 
		join.getRight() instanceof OpProjection &&
		((OpProjection)this.getLeft()).includes((OpProjection)join.getLeft()) &&
		((OpProjection)this.getRight()).includes((OpProjection)join.getRight());
	}
	
	public void merge(OpJoin join, Collection<Xpr> xprs)
	{
		((OpProjection)this.getLeft()) .merge((OpProjection)join.getLeft(),xprs);
		((OpProjection)this.getRight()).merge((OpProjection)join.getRight(),xprs);
		
	}
	
	public boolean includesLeft(OpInterface op)
	{
		
		return
		op instanceof OpProjection &&
		this.getLeft() instanceof OpProjection &&
		( ((OpProjection)this.getLeft()).includes((OpProjection)op) ||
		(	this.getLeft().getId().equals(((OpProjection)op).getId()) 
				&& ((OpProjection)op).getExpressions().containsKey("placeholder")	));
	}
	
	public boolean includesRight(OpInterface op)
	{
		return
		op instanceof OpProjection &&
		this.getRight() instanceof OpProjection &&
		((OpProjection)this.getRight()).includes((OpProjection)op);
	}	
	
	@Override
	public OpJoin copyOp()
	{
		OpInterface leftCopy = null;
		OpInterface rightCopy = null;
		if (getLeft()!=null) 
			leftCopy = getLeft();//.copyOp();
		if (getRight()!=null)
			rightCopy = getRight();//.copyOp();
		OpJoin copy = new OpJoin(this.getId(),leftCopy,rightCopy);
		copy.conditions = Lists.newArrayList();
		copy.conditions.addAll(conditions);
		return copy;
	}
	
	public OpInterface build(OpUnion union)
	{
		OpJoin join = new OpJoin(this, union);
		return join;
	}
	
	public OpInterface build(OpJoin join)
	{
		if (!(this.getLeft() instanceof OpProjection))//currentBinary.getOpLeft().getExt() == null)  //propagate
		{
			//currentBinary.setRight(buildTree(currentBinary.getRight(),join));//should be able to propagate to both sides....
			this.setLeft(this.getLeft().build(join));
			return this;
		}				
		if (!(this.getRight() instanceof OpProjection))//currentBinary.getOpLeft().getExt() == null)  //propagate
		{
			//currentBinary.setRight(buildTree(currentBinary.getRight(),join));//should be able to propagate to both sides....
			this.setRight(this.getRight().build(join));
			return this;
		}				
		if (join.includes(this))
		{
			OpJoin copy = join.copyOp();
			this.merge(join,(Collection) null);
			return this;
		}
		else if (this.includesLeft(join.getLeft()) ) //join identifiers always left handed???
		{
			OpJoin copy = join.copyOp();					
			this.setLeft(merge(copy,this.getLeft()));
			return this;
		}
		else if (this.includesRight(join.getLeft())) 
		{
			this.setRight(join);
			return this;
		}

		else					
			return this;
	
	}
	
	public OpInterface build(OpProjection proj)
	{
		if (this.getLeft()==null)
		{
			this.setLeft(proj);return this;
		}
		else if (this.getRight()==null)
		{
			this.setRight(proj);return this;
		}
		else
		{
			OpJoin join = new OpJoin(this, proj);
			return join;
		}
	}
	
	@Override 
	public OpInterface build(OpInterface newOp)
	{
		String type = newOp.getName();
		{						
			if (type.equals("join"))
			{		
				//TODO fix the regrouping of joins
				OpJoin join = (OpJoin)newOp;
				return build(join);
															 
			}	
			else if (type.equals("projection"))
			{
				OpProjection proj = (OpProjection)newOp;
				return build(proj);
			}
			else if (type.equals("selection"))
			{
				OpUnary sel = (OpUnary)newOp;
				if (sel.getId().equals(this.getLeft().getId()))
				{
					OpInterface inter = this.getLeft().build(newOp);
					this.setLeft(inter);
				}
				if (sel.getId().equals(this.getRight().getId()))
				{
					OpInterface inter = this.getRight().build(newOp);
					this.setRight(inter);
				}
				
				return this;
			}
			else if (newOp instanceof OpUnion)
			{
				return build((OpUnion)newOp);
			}
			else if (newOp instanceof OpMultiUnion)
			{
				OpMultiUnion union = (OpMultiUnion)newOp;
				OpJoin join = new OpJoin(this,union);
				return join;
			}
			else
			{
				throw new NotImplementedException("not implemented "+newOp);
			}
		}
		//return this;
	}
	
	private OpJoin merge (OpJoin j,OpInterface j2)
	{
		//TODO fix silly merge strategy
		j.setLeft(j2);
		return j;
	}
	
	
	@Override
	public OpInterface merge(OpInterface op, Collection<Xpr> xprs)
	{
		if (op instanceof OpProjection)
		{
			OpProjection proj = (OpProjection)op;
			for (Xpr xpr : xprs)
			{
				BinaryXpr bixpr = (BinaryXpr)xpr;
				VarXpr var = (VarXpr)bixpr.getLeft();
				//logger.debug("expr: "+var+"--"+proj.getExpressions().get(var.getVarName()));
				if (this.getVars().containsKey(var.getVarName()) &&
					proj.getVars().containsKey(var.getVarName()) )
				{
					if (this.getLeft().getVars().containsKey(var.getVarName()))
					{
						OpInterface newop = getLeft().merge(proj.copyOp(), xprs);
						setLeft(newop);
						logger.debug("messy join here");
						
						return this.simplify();
					}
					else if (this.getRight().getVars().containsKey(var.getVarName()))
					{
						OpInterface newop = getRight().merge(proj.copyOp(), xprs);
						setRight(newop);
						logger.debug("messy join here right");
						OpInterface res = this.simplify();
						
						return res;
					}
				}
				
				else return new OpJoin(null,this,proj.copyOp());
			}

			return this;
		}
		else 
		
		throw new NotImplementedException("No implementation for Op "+op);
	}

	@Override
	public OpInterface simplify()
	{
		if (getLeft()==null ||	getRight()==null)
			return null;
		else if (hasEqualConditions() && getLeft() instanceof OpProjection && getRight() instanceof OpProjection)
		{
			OpProjection leftProj = (OpProjection)getLeft();
			OpProjection rightProj = (OpProjection)getRight();
			if (leftProj.getRelation().getExtentName().equals(rightProj.getRelation().getExtentName()))
			{
				for (Xpr condition:conditions)
				{
					BinaryXpr bin = (BinaryXpr)condition;
					VarXpr var = (VarXpr) bin.getLeft();
					
					if (leftProj.getRelation().getUniqueIndexes().contains(var.getVarName()))
					{
						leftProj.add(rightProj);
						return leftProj;
					}
				}
			}
		}
		return this;
	}
	

	@Override
	public String getName()
	{		
		return OP_JOIN;
	}
}
