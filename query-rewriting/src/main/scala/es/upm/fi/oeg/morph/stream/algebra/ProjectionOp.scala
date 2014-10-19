package es.upm.fi.oeg.morph.stream.algebra
import collection.JavaConversions._
import es.upm.fi.oeg.morph.stream.algebra.xpr.Xpr
import es.upm.fi.oeg.morph.stream.algebra.xpr.VarXpr
import es.upm.fi.oeg.morph.stream.algebra.xpr.BinaryXpr
import es.upm.fi.oeg.morph.stream.algebra.xpr.OperationXpr
import es.upm.fi.oeg.morph.stream.algebra.xpr.FunctionXpr
import org.slf4j.LoggerFactory
import es.upm.fi.oeg.morph.stream.rewriting.QueryRewritingException

class ProjectionOp(val expressions:Map[String,Xpr], subOp:AlgebraOp, val distinct:Boolean) 
  extends UnaryOp(null,"projection",subOp) {
  private val logger= LoggerFactory.getLogger(this.getClass)
  override val id={
    val suf=if (getRelation!=null) getRelation.id else ""
    expressions.map(e=>e._2.toString)mkString("-")+suf
  }
  
  def includes(proj:ProjectionOp)=
	proj.expressions.keySet.forall{exp=>
      this.expressions.contains(exp) && this.expressions(exp).isEqual(proj.expressions(exp))
    }
	
  override def vars =
	expressions.keySet.map(k=>(k,new VarXpr(k))).toMap
  
  def getVarMappings:Map[String,Seq[String]]={
	expressions.map{e=>e._2 match {
	  case varX:VarXpr=>e._1->List(varX.varName)
	  case funcX:FunctionXpr=>e._1->funcX.varNames.toList
	  case _=>e._1->Nil
	}}
  }
	
  override def toString:String=
	return name+" ("+expressions.mkString(",")+")"
	
  override def copyOp():ProjectionOp=	{		
	val op=if (subOp!=null) subOp.copyOp
	  else null					
	new ProjectionOp(expressions, op,distinct)		
  }

  @deprecated("merge deprecated","")
  override def merge(op:AlgebraOp,xprs:Seq[Xpr]):AlgebraOp={
    throw new Exception ("biban")
	if (op==null) this
	else op match{
	  case proj:ProjectionOp=>
		this.merge(proj,xprs)
	  case _=>
		val newXprs=expressions++op.vars.entrySet.filter(e=> !expressions.contains(e.getKey)).map{e=>
		  e.getKey->e.getValue}.toMap
		new ProjectionOp(newXprs,subOp,distinct)
	}
  }
  
  override def clone(sub:AlgebraOp)=new ProjectionOp(expressions,sub,distinct)
  
  private def replaceLeaf(unary:UnaryOp,replacement:UnaryOp):UnaryOp=unary.subOp match {
    case rel:RelationOp=>unary.clone(replacement) 
    case un:UnaryOp=>unary.clone(replaceLeaf(un,replacement))
    case _=>unary.clone(replacement)
  }
  
  def merge(proj:ProjectionOp)={
    // Use the more complex subop //this is wrong, it can eliminate selections in both projs!
    proj.subOp match{
      case un:UnaryOp=>replaceLeaf(new ProjectionOp(expressions++proj.expressions,this.subOp,distinct),un)
      case _=>throw new QueryRewritingException("Cannot merge projections: invalid operator: "+proj.subOp)
    }    	    
  }

  @deprecated("Not used anymore","")
  def merge(proj:ProjectionOp,xprs:Seq[Xpr]):AlgebraOp={
    throw new Exception("this should be erased")
	logger.debug("Merging projection: "+this +" and "+proj)
	xprs.foreach{xpr=>
	  val bixpr = xpr.asInstanceOf[BinaryXpr]
	  val vari = bixpr.left.asInstanceOf[VarXpr]
	  logger.debug("expr: "+vari+"--"+proj.expressions(vari.varName))
	  if (this.expressions(vari.varName).isInstanceOf[VarXpr]){
		val var1 = this.expressions(vari.varName).asInstanceOf[VarXpr]
		val var2 = proj.expressions(vari.varName).asInstanceOf[VarXpr]
	  }
	  else if (expressions(vari.varName).isInstanceOf[OperationXpr]){
		val opr1 =this.expressions.get(vari.varName).asInstanceOf[OperationXpr]
		val opr2 =proj.expressions.get(vari.varName).asInstanceOf[OperationXpr]
		if (opr1.isEqual(opr2))
		  return add(proj)
		else
		  return null
	  }
	}
		if (proj.getRelation.extentName.equals("constants"))//TODO constants muct match, verify
		{
			if (!proj.expressions.entrySet.filter(k=>expressions.containsKey(k.getKey)).forall(e=>
			  expressions(e.getKey).isEqual(e.getValue)))
			  return null			
		}		
		else if (proj.getRelation.extentName.equals(""))//TODO fix empty relation
			return this;
		else //if (xprs!=null && xprs.size()>0)
		{
			val join = new InnerJoinOp(this.copyOp, proj.copyOp)
			logger.debug("The new join ");
			join.display
			return join
		}
		
		//logger.debug("Merged projection: "+this);
		return this;
	}

    @deprecated("not used anymore","")
	private def add(proj: ProjectionOp)={
      throw new Exception("deprecated")
	  val newXprs=expressions++proj.vars.entrySet.filter(e=> !expressions.contains(e.getKey)).map{e=>
			    e.getKey->e.getValue}.toMap
	 new ProjectionOp(newXprs,subOp.copyOp,distinct)
	}
	
	override def join(newOp:AlgebraOp):AlgebraOp={
	  if (newOp==null) null
	  else newOp match {		
	  case projection:ProjectionOp=>				
		new InnerJoinOp(this,projection)
	  case union:MultiUnionOp=>
	    new InnerJoinOp(this,union)
	  case sel:SelectionOp=>
		if (sel.subOp == null) /*only for empty selections!*/{
		  val newSel=new SelectionOp(sel.id,subOp,sel.expressions)
		  new ProjectionOp(expressions,newSel,distinct)
		}
		else this
	  case join:InnerJoinOp=>
	    throw new IllegalArgumentException("Not implemented")
		if (this.id.equals(join.left.id)){
		  if (join.left.isInstanceOf[ProjectionOp] 
				&& this.includes(join.left.asInstanceOf[ProjectionOp])) {
			new InnerJoinOp(this,join.right)//join.copyOp			
		  }
		  else  {
			logger.info("what happened here: "+this.toString)
			null
		  }			
		}
		else if (this.id.equals(join.right.id)){
		  if (join.right.isInstanceOf[ProjectionOp] 
				&& this.includes(join.right.asInstanceOf[ProjectionOp])){	
			new InnerJoinOp(join.left,this)					
		  }
		  else {	
			logger.info("what happened here2: "+this.toString)
			null
		  }
		}																				
		this
	  }
	}



}