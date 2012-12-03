package es.upm.fi.oeg.morph.stream.algebra
import org.apache.commons.lang.NotImplementedException
import es.upm.fi.oeg.morph.stream.algebra.xpr.BinaryXpr
import es.upm.fi.oeg.morph.stream.algebra.xpr.XprUtils

class InnerJoinOp (left:AlgebraOp,right:AlgebraOp)   
  extends JoinOp("join",left,right) {
  
  def hasEqualConditions:Boolean={
	if (conditions.isEmpty) return false
	else
	  conditions.filter(_.isInstanceOf[BinaryXpr]).forall(bi=>
		bi.op.equals("=") && bi.left.isEqual(bi.right))		
  }
  
  
  
  def isCompatible=(left,right) match{
    case (p1:ProjectionOp,p2:ProjectionOp)=>
      val varnames=conditions.map(c=>c.varNames).flatten
      logger.debug("compatible: "+varnames)
      
      varnames.forall{v=>println(p1.expressions(v));  
        XprUtils.canbeEqual(p1.expressions(v),p2.expressions(v))}
    case (_,_)=>true
  }
	
  def includes(join:InnerJoinOp):Boolean=(join.left,join.right) match {
    case (lp:ProjectionOp,rp:ProjectionOp)=>left.asInstanceOf[ProjectionOp].includes(lp) &&
      right.asInstanceOf[ProjectionOp].includes(rp)
    case _=>false
  }
	/*
  def merge(join:InnerJoinOp, xprs:Seq[Xpr])={
		((OpProjection)this.getLeft()) .merge((OpProjection)join.getLeft(),xprs);
		((OpProjection)this.getRight()).merge((OpProjection)join.getRight(),xprs);		
	}
	*/
	def includesLeft(op:AlgebraOp):Boolean={		
		return	op.isInstanceOf[ProjectionOp] &&
		this.left.isInstanceOf[ProjectionOp] &&
		( this.left.asInstanceOf[ProjectionOp].includes(op.asInstanceOf[ProjectionOp]) ||
		(	this.left.id.equals(op.asInstanceOf[ProjectionOp].id) 
				&& op.asInstanceOf[ProjectionOp].expressions.contains("placeholder")	))
	}
	
	def includesRight(op:AlgebraOp):Boolean={
		return op.isInstanceOf[ProjectionOp] &&
		this.right.isInstanceOf[ProjectionOp] &&
		(this.right.asInstanceOf[ProjectionOp]).includes(op.asInstanceOf[ProjectionOp]);
	}	
	/*
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
	*/
  def build(join:InnerJoinOp):AlgebraOp={
	if (!this.left.isInstanceOf[ProjectionOp]){
	  return new InnerJoinOp(left.build(join),right)
	}				
	if (!this.right.isInstanceOf[ProjectionOp]){
	  return new InnerJoinOp(left,right.build(join))
    }				
	if (join.includes(this)){			
	  return this.merge(join,null)
	}
	else if (this.includesLeft(join.left) ) //join identifiers always left handed???
	{
	  val copy = join.copyOp					
	  return this;
	}
	else if (this.includesRight(join.left)) 
	{
      return this;
	}
	else return this;
  }
	
	def build(proj:ProjectionOp):AlgebraOp={
		if (this.left==null)
		  return new InnerJoinOp(proj,right)
		else if (this.right==null)
		  return new InnerJoinOp(left,proj)
		else{
		  val join = new InnerJoinOp(this, proj)			
		  return join
		}
	}
	
	override def build(newOp:AlgebraOp):AlgebraOp =newOp match{
	  case join:InnerJoinOp=>return build(join)															 
	  case proj:ProjectionOp=>return build(proj)
	  case sel:SelectionOp=>
		if (sel.id.equals(this.left.id)){
		  val inter = this.left.build(newOp)
		  new InnerJoinOp(inter,right)
		  //this.setLeft(inter)
		}
		if (sel.id.equals(this.right.id)){
		  val inter = this.right.build(newOp)
		  new InnerJoinOp(left,inter)
		  //this.setRight(inter)
		}
		return this
			
	  //case union:OpUnion=>return build(union)
	  case union:MultiUnionOp=>
	    val join = new InnerJoinOp(this,union)
		return join		
	  case _=>throw new NotImplementedException("not implemented for "+newOp)
			
	}

	/*
	private OpJoin merge (OpJoin j,OpInterface j2)
	{
		//TODO fix silly merge strategy
		j.setLeft(j2);
		return j;
	}*/
	
	/*
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

*/
  
}