package es.upm.fi.oeg.morph.stream.algebra
import es.upm.fi.oeg.morph.stream.algebra.xpr.Xpr
import org.apache.commons.lang.NotImplementedException
import org.slf4j.LoggerFactory

trait AlgebraOp {
  val id:String
  val name:String
  def build(op:AlgebraOp):AlgebraOp
  def display(level:Int)
  def display
  def copyOp:AlgebraOp
  def merge(op:AlgebraOp,xprs:Seq[Xpr]):AlgebraOp
  def vars:Map[String,Xpr]	
}

class UnaryOp(val id:String,val name:String,val subOp:AlgebraOp) extends AlgebraOp {
  private val logger= LoggerFactory.getLogger(this.getClass)

  protected def tab(level:Int):String=
	(0 until level).map(_=>"\t").mkString	
	
  override def display{
    display(0)
  }
	
  override def display(level:Int){
	logger.warn(tab(level)+toString)
	if (subOp!= null)
	  subOp.display(level+1)						
  }
  
  override def vars:Map[String,Xpr]={
	null
  }
		
  override def toString=
	name+" "+id

  override def copyOp=
	new UnaryOp(id,name,null)
	
  override def build(op:AlgebraOp):AlgebraOp=
	throw new IllegalArgumentException("Never build an untyped operation")

  override def merge(op:AlgebraOp,xprs:Seq[Xpr]):AlgebraOp=
	throw new NotImplementedException("Merge operation not implemented for this operator "+this.name)

  def getRelation:RelationOp=
	if (subOp==null) null
	else if (subOp!=null && subOp.isInstanceOf[RelationOp])
	  subOp.asInstanceOf[RelationOp]
	else if (subOp.isInstanceOf[UnaryOp])
	  subOp.asInstanceOf[UnaryOp].getRelation
	else null
}

class BinaryOp(val id:String,val name:String,val left:AlgebraOp,val right:AlgebraOp) extends AlgebraOp {
  private val logger= LoggerFactory.getLogger(this.getClass)

  private def tab(level:Int)=
	(0 until level).map(_=>"\t").mkString	      	
	
  override def display{
	display(0)
  }

  override def display(level:Int){
	logger.warn(tab(level)+toString)
	if (left!= null)
	  left.display(level+1)
	if (right!= null)
	  right.display(level+1)
  }
  
  override def toString=
	name+" "+id
	
  private def getVarsNull(op:AlgebraOp):Map[String,Xpr]=
	if (op==null) Map()
	else op.vars
	
  def vars={
	val leftvars= getVarsNull(left)
	val rightvars= getVarsNull(right)
	val leftKeys=leftvars.keySet--rightvars.keySet
	val rightKeys=rightvars.keySet--leftvars.keySet
	val commonKeys=leftvars.keySet&rightvars.keySet
		
	val lMap=leftKeys.map(k=>k->leftvars(k)).toMap
	val rMap=rightKeys.map(k=>k->rightvars(k)).toMap
	val iMap=commonKeys.map{k=>
	  k->leftvars(k)//fix: only taking left var for intersection
	}.toMap
	lMap++rMap++iMap
	
  }
		
/*
	public OpInterface simplify()
	{
		if (this.getLeft()==null)
			return this.getRight();
		else if (this.getRight()==null)
			return this.getLeft();
		return this;
	}
*/
	
  override def copyOp=
	new BinaryOp(id, name, null, null)
	
  override def build(op:AlgebraOp):AlgebraOp=
	throw new IllegalArgumentException("never build a binary unnamed operation")
	
  override def merge(op:AlgebraOp, xprs:Seq[Xpr])=
	throw new NotImplementedException("No implementation for OpBinary "+this.name)
}


class RootOp(rootId:String,subOp:AlgebraOp) extends UnaryOp(rootId,"root",subOp){
  override def build(newOp:AlgebraOp)={
	val sub=if (subOp==null) newOp
	else subOp.build(newOp)
	new RootOp(id,sub)
  }
}	
