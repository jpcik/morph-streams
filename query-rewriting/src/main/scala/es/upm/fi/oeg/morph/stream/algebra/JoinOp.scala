package es.upm.fi.oeg.morph.stream.algebra
import collection.JavaConversions._
import es.upm.fi.oeg.morph.stream.algebra.xpr.BinaryXpr
import xpr.NullValueXpr
import es.upm.fi.oeg.morph.stream.algebra.xpr.XprUtils
import org.slf4j.LoggerFactory

abstract class JoinOp(opname:String,l:AlgebraOp,r:AlgebraOp) extends BinaryOp(null,opname,l,r){
  val conditions=generateConditions
  
  private val logger= LoggerFactory.getLogger(this.getClass)

  def generateConditions={
   if (left!=null){
     val keys=left.vars.keySet.filter(_!=null).toList
     keys.map{ key=>				
	   if (right!=null && right.vars!=null && 
	       right.vars.containsKey(key)  )
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
      
      varnames.forall{v=>
        //println(p1.expressions(v));  
        XprUtils.canbeEqual(p1.expressions(v),p2.expressions(v))}
    case (_,_)=>true
  }
  
  def isJoinOnPk:Boolean={
    (left,right) match {
      case (lProj:ProjectionOp,rProj:ProjectionOp)=>
        val varMaps=lProj.getVarMappings++rProj.getVarMappings
        val pks=lProj.getRelation.pk ++ rProj.getRelation.pk
        val condVars=this.conditions.map{cond=>
          cond.varNames.map(v=>varMaps(v)).flatten
        }.flatten.toSet
        println("condition vars "+condVars+ "--"+pks+"--"+(pks==condVars))
        pks==condVars
        //true
      case _ => false
    }
  }

}