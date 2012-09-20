package es.upm.fi.oeg.morph.stream.algebra
import es.upm.fi.oeg.morph.stream.algebra.xpr.AggXpr

class GroupOp(id:String, val groupVars:Seq[String],val aggs:Map[String,AggXpr], subOp:AlgebraOp) 
  extends UnaryOp(id,"group",subOp){
  override def toString=name+ 
    "("+groupVars.mkString(",")+")"+ "AGGS "+aggs.mkString(",")
}