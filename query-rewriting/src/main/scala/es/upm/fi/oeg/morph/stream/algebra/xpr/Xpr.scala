package es.upm.fi.oeg.morph.stream.algebra.xpr
import es.upm.fi.oeg.morph.r2rml.R2rmlUtils

trait Xpr {
  def copy:Xpr 
  def isEqual(other:Xpr):Boolean
  def varNames:Set[String] 
}

case class BinaryXpr(op:String,left:Xpr,right:Xpr) extends Xpr{
  override def toString=
	left.toString + " "+op +" "+right.toString
  override def isEqual(other:Xpr)=throw new IllegalStateException("Invalid comparison of BinaryXpr")
  override def varNames=left.varNames++right.varNames	    
  override def copy=new BinaryXpr(op,left,right)
}

object NullValueXpr extends ValueXpr("NULL")

case class ValueXpr(value:String) extends Xpr{
  override def toString=value
  
  override def isEqual(other:Xpr)=other match{
	case valXpr:ValueXpr=>this.value.equals(valXpr.value)
	case _=>false
  }

  override def varNames=Set()
  override def copy=new ValueXpr(value)
}

case class VarXpr(varName:String) extends Xpr{
  override def toString=varName
  override def isEqual(other:Xpr)=other match{
	case varXpr:VarXpr=>varName.equals(varXpr.varName)
	case _=>false
  }
  override def varNames=Set(varName)
  override def copy=new VarXpr(varName)
}


case class FunctionXpr(op:String,params:Seq[Xpr]) extends Xpr{
  override def isEqual(other:Xpr)=other match{
    case fun:FunctionXpr=>op.equals(fun.op)&& params.zip(fun.params).forall(a=>a._1.isEqual(a._2))
    case _=>false
  }
  override def varNames=params.map(_.varNames).flatten.toSet
  override def toString=op+"("+params.mkString(",")+")"
  override def copy=new FunctionXpr(op,params)
}

class ConcatXpr(params:Seq[Xpr]) extends FunctionXpr("concat",params)
class ReplaceXpr(val template:String,val vars:Seq[VarXpr]) 
  extends FunctionXpr("replace",vars++Seq(ValueXpr(template))){
  
  def replace(values:Map[String,Any])={
    var res=template
    vars.foreach(v=>res=res.replace("{"+v.varName+"}",values(v.varName).toString))
    res
  }
}

case class OperationXpr(op:String,param:Xpr) extends Xpr{
  override def toString=
    if (op.equals("constant")) "'"+param.toString+"'"
	else op+"("+param.toString+")"
	
  override def isEqual(other:Xpr)=other match{
	case oper:OperationXpr=> 
	  this.op.equals(oper.op) && this.param.isEqual(oper.param)
	case _=>false  
  }
	
  override def varNames=param.varNames
  override def copy=new OperationXpr(op, param.copy)
}


object XprUtils{
  def canbeEqual(x1:Xpr,x2:Xpr)= (x1,x2) match {
    case (rep1:ReplaceXpr,rep2:ReplaceXpr)=>
      R2rmlUtils.removeTemplateVars(rep1.template).equals(R2rmlUtils.removeTemplateVars(rep2.template))
    case (var1:VarXpr,var2:VarXpr)=> true
    case _=>true
  }
}