package es.upm.fi.dia.oeg.integration.algebra;

import java.util.Collection;

import org.apache.log4j.Logger;

import es.upm.fi.dia.oeg.integration.algebra.xpr.Xpr;

public class OpUnion extends OpBinary
{
	private static Logger logger = Logger.getLogger(OpUnion.class.getName());

	public OpUnion(OpInterface left, OpInterface right)
	{
		this(null,left,right);
	}
	public OpUnion(String id, OpInterface left, OpInterface right)
	{
		super(id, "union",left, right);
	}
	
	@Override
	public OpInterface build(OpInterface newOp)
	{
		String type =newOp.getName();		

		
			if (type.equals("projection"))
			{					
				OpProjection proj = (OpProjection)newOp;
				OpJoin join = new OpJoin(this, proj);
				return join;
				/*
				if (this.getId().equals(proj.getId()))
				{
					OpUnion union = new OpUnion(this.getId(), this, proj);
					return union;
				}
				else  //propagate to union children
				{
					OpInterface l =this.getLeft().build(proj);
					OpInterface r =this.getRight().build(proj);
					this.setLeft(l);
					this.setRight(r);
					//return currentBinary;
					return this.simplify();					
				}*/
			}
			else if (type.equals("selection"))
			{
				OpUnary sel = (OpUnary)newOp;
				if (this.getId().equals(sel.getId()))
				{
					sel.setSubOp(this);
					return sel;
				}
				OpInterface l =this.getLeft().build(newOp);
				OpInterface r = this.getRight().build(newOp);
				this.setLeft(l);
				this.setRight(r);				
				return this;
			}
				
			else if (type.equals("join"))
			{
				OpJoin join = (OpJoin)newOp;
				if (this.getLeft()!=null)
				{
				OpInterface res = this.getLeft().build(join);
				this.setLeft(res);
				}
				if (this.getRight() != null)
				{
				OpInterface res1 = this.getRight().build(join);
				this.setRight(res1);
				}
				return this;					
			}
			else if (type.equals("union"))
			{
				OpUnion union = (OpUnion)newOp;
				OpJoin join = new OpJoin(this,union);
				return join;
			}
			
		return this;

	}
	
	public OpInterface merge(OpInterface op, Collection<Xpr> xprs)
	{
		logger.debug("merging union "+op);
		OpUnion copy = new OpUnion(this.getId(),null,null);
		if (op instanceof OpUnion)
		{
			OpUnion union = (OpUnion)op;
			OpInterface l1 = getLeft().merge(union.getLeft(),xprs);
			OpInterface l2= getLeft().merge(union.getRight(),xprs);
			/*
			if (l1!=null)
			{
				l1.merge(l2);
				copy.setLeft(l1);
			}
			else
				copy.setLeft(l2);*/
			OpUnion leftU = new OpUnion(l1, l2);
			
			OpInterface r1 = getRight().merge(union.getLeft(),xprs);
			OpInterface r2= getRight().merge(union.getRight(),xprs);
			/*
			if (r1!=null)				
			{	r1.merge(r2);
				copy.setRight(r1);
			}
			else
				copy.setRight(r2);
			*/
			OpUnion rightU = new OpUnion(r1,r2);
			copy.setLeft(leftU.simplify());
			copy.setRight(rightU.simplify());
		}
		else if (op instanceof OpProjection)
		{
			OpInterface l = getLeft().merge(op,xprs);
			OpInterface r = getRight().merge(op,xprs);
			copy.setLeft(l);
			copy.setRight(r);
		}
		logger.debug("merged union ");
		//copy.display();

		return copy.simplify();
	}
}
