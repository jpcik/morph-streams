package es.upm.fi.oeg.morph.stream.gsn
import es.upm.fi.oeg.morph.stream.query.SqlQuery
import es.upm.fi.oeg.morph.stream.algebra.InnerJoinOp
import es.upm.fi.oeg.morph.stream.algebra.LeftOuterJoinOp
import es.upm.fi.oeg.morph.stream.algebra.ProjectionOp
import collection.JavaConversions._
import es.upm.fi.oeg.morph.stream.algebra.MultiUnionOp
import org.apache.commons.lang.NotImplementedException
import es.upm.fi.oeg.morph.stream.algebra.AlgebraOp
import es.upm.fi.oeg.morph.stream.algebra.xpr.OperationXpr
import es.upm.fi.oeg.morph.stream.algebra.RootOp
import es.upm.fi.oeg.morph.stream.algebra.xpr.VarXpr
import es.upm.fi.oeg.morph.stream.algebra.WindowOp
import es.upm.fi.oeg.morph.stream.algebra.RelationOp
import es.upm.fi.oeg.morph.stream.algebra.SelectionOp
import es.upm.fi.oeg.morph.stream.algebra.xpr.Xpr
import java.util.Calendar
import es.upm.fi.oeg.morph.common.TimeUnit
import java.text.SimpleDateFormat
import es.upm.fi.oeg.morph.stream.query.Modifiers
import scala.collection.mutable.ArrayBuffer
import es.upm.fi.oeg.morph.stream.algebra.xpr.BinaryXpr
import es.upm.fi.oeg.morph.stream.algebra.xpr.ReplaceXpr
import es.upm.fi.oeg.morph.stream.algebra.xpr.ConstantXpr
import es.upm.fi.oeg.morph.stream.algebra.xpr.ValueXpr

class GsnQuery(op:AlgebraOp,mods:Array[Modifiers.OutputModifier]) 
  extends SqlQuery(op,mods){   
  
  val filters=new ArrayBuffer[Filter[Any]]
  val vars:Map[String,Seq[String]]=varMappings(op)
  val algebra:AlgebraOp=op
  
  override def supportsPostProc=false
  
  private def varMappings(op:AlgebraOp):Map[String,Seq[String]]=op match{
    case root:RootOp=>varMappings(root.subOp)
    case proj:ProjectionOp=>proj.getVarMappings.filter(_._2!=null)
    case _=>Map[String,Seq[String]]()
  }
  
  override def build(op:AlgebraOp):String={
	if (op == null)	return "";
	else op match{
	  case root:RootOp=>return build(root.subOp)
      case proj:ProjectionOp=>
        val projvars=proj.getVarMappings.values.flatten.toSet.filterNot(_.equals("timed"))
        "field[0]="+projvars.mkString(",")+"&"+build(proj.subOp)
	  case win:WindowOp=> 
	    val dt= Calendar.getInstance()
	    val from=win.windowSpec.from
	    val t=TimeUnit.convertToUnit(from,win.windowSpec.fromUnit,TimeUnit.MILLISECOND).toLong
	    dt.setTimeInMillis(System.currentTimeMillis -t )
	    val df=new SimpleDateFormat("dd/MM/yyyy+HH:mm:ss")
        "vs[0]="+win.extentName+"&from="+df.format(dt.getTime)+"&gogog="+t
      case rel:RelationOp=> "vs[0]="+rel.extentName
	  case sel:SelectionOp=>
	    filters++=sel.expressions.toSeq.map(e=>getFilter(e))
	    build(sel.subOp)
	  case union:MultiUnionOp=>
		return union.children.values.map(col=>build(col)).mkString(" UNION ")
	  case join:LeftOuterJoinOp=>throw new NotImplementedException("NYI Left Join")
	  case join:InnerJoinOp=>throw new NotImplementedException("NYI Join")		
	  case _=>throw new Exception("Unsupported operator: "+op)
	}
  }

  def getFilter(e:Xpr):Filter[Any]=e match{
    case bin:BinaryXpr=>
      (bin.left,bin.right) match{
        case (repl:ReplaceXpr,const:ConstantXpr)=>          
          var str=const.evaluate
          repl.templateParts.foreach(t=>str=str.replace(t,"&&&"))          
          new Filter(bin.op,repl.params.map(_.varNames).flatten,str.split("&&&").filterNot(_.length==0))
        case (v:VarXpr,vl:ValueXpr)=>
          new Filter(bin.op,Seq(v.varName),Seq(vl))
        case _=>throw new NotImplementedError("Filter not available: "+bin)
      }      
    case _=>throw new NotImplementedError("Filter not available: "+e)
  }

}
  
class Filter[T](val op:String,val fields:Seq[String],val values:Seq[T]){    
  def index(fnames:Seq[String]):Seq[Int]={
    val indexes=fields.map(fn=>fnames.indexOf(fn))
    println("indexes "+indexes)
    indexes
  }
   
  def filter(tuple:Array[Object],index:Iterator[Int]):Boolean={
    op match{
      case "="=> values.map{t=>tuple(index.next).equals(t) }.reduce(_&&_)
    }
     
  }
  override def toString=op+ " "+values.mkString(",")
}
