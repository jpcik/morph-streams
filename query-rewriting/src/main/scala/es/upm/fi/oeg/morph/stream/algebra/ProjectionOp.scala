package es.upm.fi.oeg.morph.stream.algebra
import com.weiglewilczek.slf4s.Logging
import com.google.common.collect.Maps
import es.upm.fi.dia.oeg.integration.algebra.OpUnion
import collection.JavaConversions._
import es.upm.fi.oeg.morph.stream.algebra.xpr.Xpr
import es.upm.fi.oeg.morph.stream.algebra.xpr.VarXpr
import es.upm.fi.oeg.morph.stream.algebra.xpr.BinaryXpr
import es.upm.fi.oeg.morph.stream.algebra.xpr.OperationXpr
import es.upm.fi.oeg.morph.stream.algebra.xpr.FunctionXpr

class ProjectionOp(id:String,val expressions:Map[String,Xpr], subOp:AlgebraOp) 
  extends UnaryOp(id,"projection",subOp) with Logging{
	
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
	return name+" "+id+"("+expressions.mkString(",")+")"
	
  override def copyOp():ProjectionOp=	{		
	val op=if (subOp!=null) subOp.copyOp
	  else null					
	new ProjectionOp(id, expressions, op)		
  }

  override def merge(op:AlgebraOp,xprs:Seq[Xpr]):AlgebraOp={
	if (op==null) this
	else op match{
	  case proj:ProjectionOp=>
		this.merge(proj,xprs)
	  case _=>
		val newXprs=expressions++op.vars.entrySet.filter(e=> !expressions.contains(e.getKey)).map{e=>
		  e.getKey->e.getValue}.toMap
		new ProjectionOp(id,newXprs,subOp)
	}
  }
  
  def merge(proj:ProjectionOp)={
    new ProjectionOp(id,expressions++proj.expressions,subOp)
  }

  def merge(proj:ProjectionOp,xprs:Seq[Xpr]):AlgebraOp={
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

	private def removeColumn(template:String):String={
	    val (i,j)=(template.indexOf('{'),template.indexOf('}'))
        template.substring(0,i)+template.substring(j+1,template.length)
	}
	
	private def add(proj: ProjectionOp)={
	  val newXprs=expressions++proj.vars.entrySet.filter(e=> !expressions.contains(e.getKey)).map{e=>
			    e.getKey->e.getValue}.toMap
	 new ProjectionOp(id,newXprs,subOp.copyOp)
	}
	
	override def build(newOp:AlgebraOp):AlgebraOp=
	  if (this.id.equals("mainProjection"))//TODO this is hacky
	  {
		if (subOp==null)	return newOp
		else return subOp.build(newOp)			
	  }
	  else newOp match{		
	  case projection:ProjectionOp=>				
		return new InnerJoinOp(this,projection)
	  case union:MultiUnionOp=>
	    return new InnerJoinOp(this,union)
	  case sel:SelectionOp=>
		if (sel.subOp == null) //only for empty selections!
		{
		  val newSel=new SelectionOp(sel.id,subOp,sel.expressions)
		  return new ProjectionOp(id,expressions,newSel)
		}
		return this
	  case join:InnerJoinOp=>
		if (this.id.equals(join.left.id)){
		  if (join.left.isInstanceOf[ProjectionOp]
				&& (join.left.asInstanceOf[ProjectionOp]).expressions.containsKey("placeholder"))
		  {
		    logger.info(join.id);
			val joinCopy = new InnerJoinOp(this,join.right)
			return joinCopy;
		  }
		  else if (join.left.isInstanceOf[ProjectionOp] 
				&& this.includes(join.left.asInstanceOf[ProjectionOp]))
		  {
			val copy =new InnerJoinOp(this,join.right)//join.copyOp
			return copy;
		  }
		  else
		  {
			logger.info("what happened here: "+this.toString());
			logger.info("bap: "+join.right.id);	
			return null
		  }			
		}
		else if (this.id.equals(join.right.id))
			{
				if (join.right.isInstanceOf[ProjectionOp] 
							&& this.includes(join.right.asInstanceOf[ProjectionOp]))
				{	
					val copy = new InnerJoinOp(join.left,this)
					//copy.setRight(this);
					return copy;
				}
				else				
				{	
					logger.info("what happened here2: "+this.toString());
					logger.info("bap2: "+join.right.id);
					return null
				}
			}									
										
		
		
		return this;
	}



}