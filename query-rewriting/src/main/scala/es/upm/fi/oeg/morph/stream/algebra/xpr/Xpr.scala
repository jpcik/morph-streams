package es.upm.fi.oeg.morph.stream.algebra.xpr
import es.upm.fi.oeg.morph.r2rml.R2rmlUtils
import es.upm.fi.oeg.siq.tools.URLTools
import org.apache.commons.lang.NotImplementedException

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


abstract class FunctionXpr(val op:String,val params:Seq[Xpr]) extends Xpr{
  def evaluate(values:Map[String,Any]):Any
  override def isEqual(other:Xpr)=other match{
    case fun:FunctionXpr=>op.equals(fun.op)&& params.zip(fun.params).forall(a=>a._1.isEqual(a._2))
    case _=>false
  }
  override def varNames=params.map(_.varNames).flatten.toSet
  override def toString=op+"("+params.mkString(",")+")"
  override def copy:Xpr=null//new FunctionXpr(op,params)
}

case class VarXpr(varName:String) extends FunctionXpr("var",Seq()){
  //def this(varName:String)=this(varName,null)
  override def toString=varName
  override def isEqual(other:Xpr)=other match{
	case varXpr:VarXpr=>varName.equals(varXpr.varName)
	case _=>false
  }
  override def varNames=Set(varName)
  override def copy=new VarXpr(varName)
  override def evaluate(values:Map[String,Any])= values(varName)
}

object UnassignedVarXpr extends VarXpr("?")

class ReplaceXpr(val template:String,val vars:Seq[FunctionXpr]) 
  extends FunctionXpr("replace",vars++Seq(ValueXpr(template))){
  def split=R2rmlUtils.extractTemplateVals(template) zip vars
  override def evaluate(values:Map[String,Any])={
    var res=template
    split.foreach(v=>res=res.replace("{"+v._1+"}",v._2.evaluate(values).toString))
    res
  }
}

class PercentEncodeXpr(val vari:VarXpr) extends FunctionXpr("pencode",Seq(vari)){
  override def evaluate(values:Map[String,Any])=
    URLTools.encode(values(vari.varName).toString) 
}

class ConstantXpr(par:String) extends OperationXpr("constant",ValueXpr(par)) {
  override def toString="'"+par+"'"
  override def evaluate=par
}

case class OperationXpr(op:String,param:Xpr) extends Xpr{
  override def toString=
    //if (op.equals("constant")) "'"+param.toString+"'"
	op+"("+param.toString+")"
	
  override def isEqual(other:Xpr)=other match{
	case oper:OperationXpr=> 
	  this.op.equals(oper.op) && this.param.isEqual(oper.param)
	case _=>false  
  }
	
  override def varNames=param.varNames
  override def copy=new OperationXpr(op, param.copy)
  
  def evaluate:Object= throw new NotImplementedException("evaluation of operation not impl. "+op) 
}


object XprUtils{
  def canbeEqual(x1:Xpr,x2:Xpr)= (x1,x2) match {
    case (rep1:ReplaceXpr,rep2:ReplaceXpr)=>
      R2rmlUtils.removeTemplateVars(rep1.template).equals(R2rmlUtils.removeTemplateVars(rep2.template))
    case (var1:VarXpr,var2:VarXpr)=> true
    case _=>true
  }
}