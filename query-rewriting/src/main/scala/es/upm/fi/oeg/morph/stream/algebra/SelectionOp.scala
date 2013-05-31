package es.upm.fi.oeg.morph.stream.algebra
import es.upm.fi.oeg.morph.stream.algebra.xpr.Xpr

class SelectionOp(selectionId:String,subOp:AlgebraOp,val expressions:Set[Xpr]) 
  extends UnaryOp(selectionId,"selection",subOp){

  def selectionVarNames=
	expressions.map(xpr=>xpr.varNames).flatten
	
  override def toString=
	super.toString +" ("+ expressions.toString+")"
	
  override def copyOp={
	val sub= if (subOp!=null) subOp.copyOp
	  else null
	new SelectionOp(id, sub,expressions)
  }
	
  override def build(newOp:AlgebraOp)=
	new SelectionOp(id,subOp.build(newOp),expressions)
	
  def simplify=
    if (subOp==null) null
    else this
}