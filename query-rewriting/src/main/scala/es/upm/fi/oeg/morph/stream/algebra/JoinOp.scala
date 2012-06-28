package es.upm.fi.oeg.morph.stream.algebra
import collection.JavaConversions._
import es.upm.fi.oeg.morph.stream.algebra.xpr.BinaryXpr

abstract class JoinOp(opname:String,l:AlgebraOp,r:AlgebraOp) extends BinaryOp(null,opname,l,r){
  val conditions=generateConditions
  
  def generateConditions={
   if (left!=null){
     val keys=left.vars.keySet.filter(_!=null).toList
     keys.map{ key=>				
	   if (right!=null && right.vars!=null && right.vars.containsKey(key))
		 new BinaryXpr("=",left.vars(key),right.vars(key))					
	   else null
	 }.filter(_!=null)
   }
   else List()
  }
    
  override def toString:String={
    val cond=conditions.map(x=>x.toString).mkString(" ")
	return super.toString+" ("+cond+")"
  }
      
}